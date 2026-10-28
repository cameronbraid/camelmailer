//! Resolve `worker.servers` / `worker.exclude_servers` into the
//! [`ServerFilter`] the worker's queues dequeue with.
//!
//! Permalinks are resolved once at startup. A permalink that names no server
//! is an error rather than an empty selection: a typo in `worker.servers`
//! would otherwise leave a worker idle, and one in `worker.exclude_servers`
//! would let a general worker take the work it was meant to leave alone.

use camelmailer_config::parse_server_permalink;
use camelmailer_core::{AdminStore, Id};
use camelmailer_db::ServerFilter;

pub async fn resolve_server_filter(
    store: &dyn AdminStore,
    worker: &camelmailer_config::Worker,
) -> Result<ServerFilter, String> {
    match (worker.servers.is_empty(), worker.exclude_servers.is_empty()) {
        (true, true) => Ok(ServerFilter::All),
        (false, true) => Ok(ServerFilter::Only(
            resolve_ids(store, "worker.servers", &worker.servers).await?,
        )),
        (true, false) => Ok(ServerFilter::Except(
            resolve_ids(store, "worker.exclude_servers", &worker.exclude_servers).await?,
        )),
        (false, false) => Err(
            "worker.servers and worker.exclude_servers cannot both be set; \
             run separate workers for each selection"
                .into(),
        ),
    }
}

async fn resolve_ids(
    store: &dyn AdminStore,
    key: &str,
    entries: &[String],
) -> Result<Vec<Id>, String> {
    let mut ids = Vec::with_capacity(entries.len());
    for entry in entries {
        let (organization, server) = parse_server_permalink(entry).ok_or_else(|| {
            format!("{key}: {entry:?} must be an organization/server permalink pair")
        })?;
        let organization = store
            .organization_by_permalink(organization)
            .await
            .map_err(|error| format!("{key}: looking up {entry:?}: {error}"))?
            .ok_or_else(|| format!("{key}: organization {organization:?} not found"))?;
        let server = store
            .server_by_permalink(organization.id, server)
            .await
            .map_err(|error| format!("{key}: looking up {entry:?}: {error}"))?
            .ok_or_else(|| format!("{key}: server {entry:?} not found"))?;
        if !ids.contains(&server.id) {
            ids.push(server.id);
        }
    }
    Ok(ids)
}

#[cfg(test)]
mod tests {
    use super::*;
    use camelmailer_core::{MemoryStore, NewOrganization, NewServer, ServerMode};

    async fn store_with_servers() -> (MemoryStore, Id, Id) {
        let store = MemoryStore::new();
        let organization = store
            .create_organization(NewOrganization {
                name: "DriveNow".into(),
                permalink: "drivenow".into(),
            })
            .await
            .unwrap();
        let mut ids = vec![];
        for permalink in ["bulk", "transact"] {
            let server = store
                .create_server(NewServer {
                    organization_id: organization.id,
                    name: permalink.into(),
                    permalink: permalink.into(),
                    mode: ServerMode::Live,
                })
                .await
                .unwrap();
            ids.push(server.id);
        }
        (store, ids[0], ids[1])
    }

    fn worker(servers: &[&str], exclude_servers: &[&str]) -> camelmailer_config::Worker {
        camelmailer_config::Worker {
            servers: servers.iter().map(|s| s.to_string()).collect(),
            exclude_servers: exclude_servers.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn no_selection_takes_every_server() {
        let (store, _, _) = store_with_servers().await;
        let filter = resolve_server_filter(&store, &worker(&[], &[]))
            .await
            .unwrap();
        assert_eq!(filter, ServerFilter::All);
    }

    #[tokio::test]
    async fn servers_resolve_to_only() {
        let (store, bulk, _) = store_with_servers().await;
        let filter = resolve_server_filter(&store, &worker(&["drivenow/bulk"], &[]))
            .await
            .unwrap();
        assert_eq!(filter, ServerFilter::Only(vec![bulk]));
    }

    #[tokio::test]
    async fn exclude_servers_resolve_to_except_without_duplicates() {
        let (store, bulk, transact) = store_with_servers().await;
        let filter = resolve_server_filter(
            &store,
            &worker(
                &[],
                &["drivenow/bulk", "drivenow/transact", "drivenow/bulk"],
            ),
        )
        .await
        .unwrap();
        assert_eq!(filter, ServerFilter::Except(vec![bulk, transact]));
    }

    #[tokio::test]
    async fn unknown_permalinks_are_errors() {
        let (store, _, _) = store_with_servers().await;
        let error = resolve_server_filter(&store, &worker(&["drivenow/missing"], &[]))
            .await
            .unwrap_err();
        assert!(
            error.contains("server \"drivenow/missing\" not found"),
            "{error}"
        );

        let error = resolve_server_filter(&store, &worker(&[], &["other/bulk"]))
            .await
            .unwrap_err();
        assert!(
            error.contains("organization \"other\" not found"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn both_selections_are_rejected() {
        let (store, _, _) = store_with_servers().await;
        let error =
            resolve_server_filter(&store, &worker(&["drivenow/bulk"], &["drivenow/transact"]))
                .await
                .unwrap_err();
        assert!(error.contains("cannot both be set"), "{error}");
    }
}
