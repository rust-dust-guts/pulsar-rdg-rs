//! Bookie rack-placement administration — `/admin/v2/bookies`.
//!
//! Mirrors `org.apache.pulsar.client.admin.Bookies`.

use std::collections::BTreeMap;

use reqwest::Method;

use crate::{
    admin::{
        clusters::NO_BODY,
        encode_segment,
        models::{BookieInfo, BookiesClusterInfo},
        AdminClient,
    },
    Error,
};

/// Rack placement for every bookie, keyed by group then bookie address.
pub type BookiesRackConfiguration = BTreeMap<String, BTreeMap<String, BookieInfo>>;

/// Handle for the `bookies` group of admin operations.
///
/// Obtained from [`AdminClient::bookies`]. Grouping mirrors the Java admin
/// client's separate interfaces and keeps same-named operations on different
/// resource kinds (a namespace retention policy vs a topic one) distinct.
pub struct Bookies<'a> {
    pub(crate) client: &'a AdminClient,
}

impl Bookies<'_> {
    fn bookies_url(&self, segments: &[&str]) -> String {
        let mut all = vec!["bookies"];
        all.extend_from_slice(segments);
        self.client.url(&all)
    }

    /// Lists every bookie known to the cluster.
    pub async fn get_bookies(&self) -> Result<BookiesClusterInfo, Error> {
        let url = self.bookies_url(&["all"]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Gets rack placement for all bookies.
    pub async fn get_bookies_rack_info(&self) -> Result<BookiesRackConfiguration, Error> {
        let url = self.bookies_url(&["racks-info"]);
        self.client.send_json(Method::GET, &url, &[], NO_BODY).await
    }

    /// Gets rack placement for one bookie, or `None` if it has none set.
    pub async fn get_bookie_rack_info(
        &self,
        bookie_address: &str,
    ) -> Result<Option<BookieInfo>, Error> {
        let url = self.bookies_url(&["racks-info", &encode_segment(bookie_address)]);
        self.client
            .send_json_absent_on_404(Method::GET, &url, &[], NO_BODY)
            .await
    }

    /// Sets rack placement for one bookie within `group`.
    pub async fn update_bookie_rack_info(
        &self,
        bookie_address: &str,
        group: &str,
        info: &BookieInfo,
    ) -> Result<(), Error> {
        let url = self.bookies_url(&["racks-info", &encode_segment(bookie_address)]);
        self.client
            .send_empty(
                Method::POST,
                &url,
                &[("group", group.to_string())],
                Some(info),
            )
            .await
    }

    /// Removes rack placement for one bookie.
    pub async fn delete_bookie_rack_info(&self, bookie_address: &str) -> Result<(), Error> {
        let url = self.bookies_url(&["racks-info", &encode_segment(bookie_address)]);
        self.client
            .send_empty(Method::DELETE, &url, &[], NO_BODY)
            .await
    }
}
