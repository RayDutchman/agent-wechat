//! Shared image-quality policy used by the V2 media reader.

use crate::tools::wechat_media::ImageQuality;

pub(crate) fn image_quality_rank(quality: Option<&str>) -> u8 {
    match quality {
        Some("full") => 3,
        Some("standard") => 2,
        Some("thumbnail") => 1,
        _ => 0,
    }
}

pub(crate) fn needs_native_transfer(quality: ImageQuality, local_type: i64, advertised: &str) -> bool {
    local_type != 3
        || (quality != ImageQuality::Standard
            && (quality != ImageQuality::Best || advertised == "full"))
}
