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

#[cfg(test)]
mod image_quality_tests {
    use super::{image_quality_rank, needs_native_transfer};
    use crate::tools::wechat_media::ImageQuality;

    #[test]
    fn best_available_quality_orders_full_standard_thumbnail() {
        assert!(image_quality_rank(Some("full")) > image_quality_rank(Some("standard")));
        assert!(image_quality_rank(Some("standard")) > image_quality_rank(Some("thumbnail")));
        assert_eq!(image_quality_rank(None), 0);
    }

    #[test]
    fn standard_images_use_cache_not_full_image_transfer() {
        assert!(!needs_native_transfer(ImageQuality::Best, 3, "standard"));
        assert!(!needs_native_transfer(ImageQuality::Best, 3, "thumbnail"));
        assert!(!needs_native_transfer(ImageQuality::Standard, 3, "full"));
        assert!(needs_native_transfer(ImageQuality::Best, 3, "full"));
        assert!(needs_native_transfer(ImageQuality::Best, 43, "standard"));
    }
}
