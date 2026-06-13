use futures::{SinkExt as _, StreamExt as _};

#[derive(serde::Deserialize, Debug)]
struct Config {
    labeler_bind: std::net::SocketAddr,
    postgres_url: String,
    empty_cursor_is_zero: bool,
}

#[derive(Copy, Clone)]
enum Message<'a> {
    Labels { seq: i64, labels: &'a [&'a [u8]] },
    Error { error: EventStreamError },
}

#[derive(Copy, Clone)]
enum EventStreamError {
    FutureCursor,
}

impl EventStreamError {
    pub const fn as_str(&self) -> &str {
        match self {
            Self::FutureCursor => "FutureCursor",
        }
    }
}

fn encode_message(msg: Message) -> Vec<u8> {
    let mut writer = minicbor::Encoder::new(vec![]);

    match msg {
        Message::Labels { seq, labels } => {
            // {"t": "#labels", "op": 1}
            writer
                .map(2)
                .unwrap()
                //
                .str("t")
                .unwrap()
                .str("#labels")
                .unwrap()
                //
                .str("op")
                .unwrap()
                .i64(1)
                .unwrap();

            // com.atproto.label.subscribeLabels#labels
            writer
                .map(2)
                .unwrap()
                //
                .str("seq")
                .unwrap()
                .i64(seq)
                .unwrap()
                //
                .str("labels")
                .unwrap()
                .array(labels.len() as u64)
                .unwrap();
            for label in labels {
                writer.writer_mut().extend(*label);
            }
        }

        Message::Error { error } => {
            // {"op": -1}
            writer
                .map(1)
                .unwrap()
                //
                .str("op")
                .unwrap()
                .i64(-1)
                .unwrap();

            // {"error": error}
            writer
                .map(1)
                .unwrap()
                //
                .str("error")
                .unwrap()
                .str(error.as_str())
                .unwrap();
        }
    }

    writer.into_writer()
}

async fn subscribe_labels(
    mut notify_recv: tokio::sync::watch::Receiver<()>,
    db_pool: sqlx::PgPool,
    empty_cursor_is_zero: bool,
    ws: axum::extract::ws::WebSocketUpgrade,
    axum_extra::extract::Query(params): axum_extra::extract::Query<
        atrium_api::com::atproto::label::subscribe_labels::ParametersData,
    >,
) -> axum::response::Response {
    ws.on_upgrade(move |socket: axum::extract::ws::WebSocket| async move {
        metrics::gauge!("num_subscriptions").increment(1);

        let (mut sink, mut stream) = socket.split();

        match async {
            let mut seq = {
                let mut db_conn = db_pool.acquire().await?;
                sqlx::query_scalar!(
                    r#"
                    SELECT COALESCE((SELECT MAX(seq) FROM labels), 0) AS "seq!"
                    "#
                )
                .fetch_one(&mut *db_conn)
                .await?
            };

            if let Some(cursor) = params.cursor {
                if cursor > seq {
                    log::info!(
                        "got websocket subscriber, cursor = {:?}, seq = {} (future cursor error)",
                        params.cursor,
                        seq
                    );
                    sink.send(axum::extract::ws::Message::Binary(
                        encode_message(Message::Error {
                            error: EventStreamError::FutureCursor,
                        })
                        .into(),
                    ))
                    .await?;
                    return Ok::<_, anyhow::Error>(());
                }
                seq = cursor;
            } else if empty_cursor_is_zero {
                seq = 0;
            }

            log::info!(
                "got websocket subscriber, cursor = {:?}, seq = {}",
                params.cursor,
                seq
            );

            loop {
                tokio::select! {
                    msg = stream.next() => {
                        let Some(msg) = msg else {
                            break;
                        };
                        let _ = msg?;
                    }
                    msg = notify_recv.changed() => {
                        msg?;
                        let mut db_conn = db_pool.acquire().await?;

                        let mut rows = sqlx::query!(
                            r#"
                            SELECT seq, payload
                            FROM labels
                            WHERE seq > $1
                            "#,
                            seq
                        )
                        .fetch(&mut *db_conn);

                        while let Some(row) = rows.next().await {
                            let row = row?;

                            sink.send(axum::extract::ws::Message::Binary(
                                encode_message(Message::Labels {
                                    seq: row.seq,
                                    labels: &[&row.payload],
                                }).into(),
                            ))
                            .await?;

                            seq = row.seq;
                        }
                    }
                };
            }

            Ok::<_, anyhow::Error>(())
        }
        .await
        {
            Ok(_) => {}
            Err(err) => {
                log::error!("subscribeLabels error: {err}");
            }
        }

        log::info!("websocket subscriber disconnected");
        metrics::gauge!("num_subscriptions").decrement(1);
    })
}

/// Liveness check for the ingester. `jetstream_cursor.cursor` is the microsecond
/// timestamp of the last firehose event the ingester committed; if it stops
/// advancing, ingestion has stalled. Returns 200 when the cursor is fresh,
/// 503 (with a short diagnostic body) otherwise so an external probe can alert.
async fn health(db_pool: sqlx::PgPool) -> axum::response::Response {
    use axum::response::IntoResponse as _;

    // Max acceptable lag between now and the last processed firehose event.
    const MAX_LAG_SECS: i64 = 600;

    let cursor: Result<Option<i64>, sqlx::Error> =
        sqlx::query_scalar("SELECT cursor FROM jetstream_cursor")
            .fetch_optional(&db_pool)
            .await;

    match cursor {
        Ok(Some(cursor_us)) => {
            let now_us = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_micros() as i64)
                .unwrap_or(0);
            let lag_secs = (now_us - cursor_us) / 1_000_000;
            let body = format!("cursor_us={cursor_us} lag_secs={lag_secs}\n");
            let status = if lag_secs <= MAX_LAG_SECS {
                axum::http::StatusCode::OK
            } else {
                axum::http::StatusCode::SERVICE_UNAVAILABLE
            };
            (status, body).into_response()
        }
        Ok(None) => (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "no jetstream_cursor row\n",
        )
            .into_response(),
        Err(err) => (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            format!("db error: {err}\n"),
        )
            .into_response(),
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    metrics::describe_gauge!("num_subscriptions", "number of active subscriptions");

    env_logger::init();

    let config: Config = config::Config::builder()
        .add_source(config::File::with_name("config.toml"))
        .set_default("labeler_bind", "127.0.0.1:3001")?
        .set_default("empty_cursor_is_zero", false)?
        .build()?
        .try_deserialize()?;
    log::info!("config: {config:?}");

    let (notify_send, notify_recv) = tokio::sync::watch::channel(());

    let db_pool = sqlx::PgPool::connect(&config.postgres_url).await?;

    let mut pg_listener = sqlx::postgres::PgListener::connect_with(
        &sqlx::pool::PoolOptions::<sqlx::Postgres>::new()
            .max_connections(1)
            .max_lifetime(None)
            .idle_timeout(None)
            .connect_with((*db_pool.connect_options()).clone())
            .await?,
    )
    .await?;
    pg_listener.ignore_pool_close_event(true);
    pg_listener.listen("labels").await?;

    let listener = tokio::net::TcpListener::bind(&config.labeler_bind).await?;

    let metrics_handle =
        metrics_exporter_prometheus::PrometheusBuilder::new().install_recorder()?;

    let app = axum::Router::new()
        .nest(
            "/xrpc",
            axum::Router::new().route(
                &format!(
                    "/{}",
                    atrium_api::com::atproto::label::subscribe_labels::NSID
                ),
                axum::routing::get({
                    let mut notify_recv = notify_recv.clone();
                    let db_pool = db_pool.clone();
                    move |ws, query| {
                        notify_recv.mark_changed();
                        subscribe_labels(
                            notify_recv,
                            db_pool,
                            config.empty_cursor_is_zero,
                            ws,
                            query,
                        )
                    }
                }),
            ),
        )
        .route(
            "/metrics",
            axum::routing::get(|| async move { metrics_handle.render() }),
        )
        .route(
            "/health",
            axum::routing::get({
                let db_pool = db_pool.clone();
                move || health(db_pool.clone())
            }),
        )
        .route("/", axum::routing::get(|| async { ">:3" }));

    tokio::try_join!(
        async {
            loop {
                pg_listener.recv().await?;
                notify_send.send(())?;
            }

            #[allow(unreachable_code)]
            Ok::<_, anyhow::Error>(())
        },
        async {
            // Serve labeler.
            axum::serve(listener, app).await?;
            unreachable!();

            #[allow(unreachable_code)]
            Ok::<_, anyhow::Error>(())
        }
    )?;

    Ok(())
}
