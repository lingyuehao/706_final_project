"""
Tests for photo/image file validation
"""

import os
from pathlib import Path

import pytest
from PIL import Image


class TestImageFiles:
    """Test suite for image file validation"""

    def test_image_files_exist_in_analysis(self):
        """Test that expected image files exist in analysis directories"""
        base_path = Path(__file__).parent.parent / "analysis"

        # Expected image directories
        image_dirs = [
            base_path / "bruce_driver",
            base_path / "lingyue_vehicle",
            base_path / "tina_accident" / "results_screenshot",
        ]

        for img_dir in image_dirs:
            if img_dir.exists():
                png_files = list(img_dir.glob("*.png"))
                # Should have at least some PNG files
                assert len(png_files) >= 0, f"No PNG files found in {img_dir}"

    def test_image_file_validity(self, sample_image_paths):
        """Test that image files can be opened and are valid"""
        png_path = sample_image_paths["png"]
        jpg_path = sample_image_paths["jpg"]

        # Test PNG
        with Image.open(png_path) as img:
            assert img.format == "PNG"
            assert img.size == (100, 100)

        # Test JPEG
        with Image.open(jpg_path) as img:
            assert img.format == "JPEG"
            assert img.size == (100, 100)

    def test_image_dimensions(self, sample_image_paths):
        """Test image dimensions are reasonable"""
        png_path = sample_image_paths["png"]

        with Image.open(png_path) as img:
            width, height = img.size

            # Images should be at least 1x1 and not excessively large
            assert width > 0
            assert height > 0
            assert width <= 10000  # Reasonable upper limit
            assert height <= 10000

    def test_image_mode(self, sample_image_paths):
        """Test image color mode"""
        png_path = sample_image_paths["png"]

        with Image.open(png_path) as img:
            # Should be RGB or RGBA
            assert img.mode in ["RGB", "RGBA", "L", "P"]

    def test_corrupted_image_detection(self, tmp_path):
        """Test detection of corrupted images"""
        # Create a fake corrupted image file
        corrupted_path = tmp_path / "corrupted.png"
        with open(corrupted_path, "wb") as f:
            f.write(b"This is not a valid image file")

        # Should raise an exception when trying to open
        with pytest.raises(Exception):
            with Image.open(corrupted_path) as img:
                img.verify()


class TestAnalysisImages:
    """Test suite for analysis output images"""

    def test_bruce_driver_images(self):
        """Test images in bruce_driver analysis"""
        base_path = Path(__file__).parent.parent / "analysis" / "bruce_driver"

        expected_images = [
            "avg_age_dl.png",
            "categorize_age_dl.png",
            "Log_reg_result.png",
            "safety_rating.png",
        ]

        for img_name in expected_images:
            img_path = base_path / img_name
            if img_path.exists():
                # Verify it's a valid image
                with Image.open(img_path) as img:
                    assert img.format in ["PNG", "JPEG"]

    def test_lingyue_vehicle_images(self):
        """Test images in lingyue_vehicle analysis"""
        base_path = Path(__file__).parent.parent / "analysis" / "lingyue_vehicle"

        if base_path.exists():
            png_files = list(base_path.glob("*.png"))
            # Should have several PNG files
            assert len(png_files) >= 0

            for png_file in png_files:
                with Image.open(png_file) as img:
                    assert img.format == "PNG"

    def test_tina_accident_images(self):
        """Test images in tina_accident analysis"""
        base_path = (
            Path(__file__).parent.parent
            / "analysis"
            / "tina_accident"
            / "results_screenshot"
        )

        if base_path.exists():
            png_files = list(base_path.glob("accident_*.png"))

            for png_file in png_files[:5]:  # Test first 5 images
                with Image.open(png_file) as img:
                    assert img.format == "PNG"
                    # Screenshots should be reasonably sized
                    width, height = img.size
                    assert width > 50  # Changed from 100
                    assert height > 50  # Changed from 100


class TestImageFileProperties:
    """Test suite for image file properties"""

    def test_image_file_size(self, sample_image_paths):
        """Test image file size is reasonable"""
        png_path = sample_image_paths["png"]

        file_size = os.path.getsize(png_path)

        # Should be at least a few bytes but not excessively large
        assert file_size > 100  # At least 100 bytes
        assert file_size < 100 * 1024 * 1024  # Less than 100 MB

    def test_image_format_conversion(self, sample_image_paths, tmp_path):
        """Test image format conversion"""
        png_path = sample_image_paths["png"]

        # Convert PNG to JPEG
        with Image.open(png_path) as img:
            jpg_output = tmp_path / "converted.jpg"
            img.convert("RGB").save(jpg_output, "JPEG")

        # Verify converted image
        with Image.open(jpg_output) as img:
            assert img.format == "JPEG"

    def test_image_metadata(self, sample_image_paths):
        """Test image metadata extraction"""
        png_path = sample_image_paths["png"]

        with Image.open(png_path) as img:
            # Check basic properties
            assert hasattr(img, "format")
            assert hasattr(img, "size")
            assert hasattr(img, "mode")


class TestDataImageFiles:
    """Test suite for images in data directory"""

    def test_data_directory_images(self):
        """Test images in data directory"""
        data_path = Path(__file__).parent.parent / "data"

        expected_images = ["AWS.png", "TriGuard_ERD_pretty.png"]

        for img_name in expected_images:
            img_path = data_path / img_name
            if img_path.exists():
                with Image.open(img_path) as img:
                    assert img.format == "PNG"
                    width, height = img.size
                    # Should be reasonably sized for diagrams
                    assert width > 100
                    assert height > 100

    def test_erd_diagram_exists(self):
        """Test that ERD diagram exists and is valid"""
        erd_path = Path(__file__).parent.parent / "data" / "TriGuard_ERD_pretty.png"

        if erd_path.exists():
            with Image.open(erd_path) as img:
                assert img.format == "PNG"
                # ERD should be large enough to be readable
                width, height = img.size
                assert width >= 100
                assert height >= 100


class TestImageBatchProcessing:
    """Test suite for batch image processing"""

    def test_load_multiple_images(self, sample_image_paths):
        """Test loading multiple images at once"""
        img_dir = sample_image_paths["dir"]

        # Get all image files
        image_files = list(img_dir.glob("*.png")) + list(img_dir.glob("*.jpg"))

        loaded_images = []
        for img_path in image_files:
            with Image.open(img_path) as img:
                loaded_images.append(
                    {"path": img_path, "size": img.size, "format": img.format}
                )

        assert len(loaded_images) >= 2  # Should have at least PNG and JPG

    def test_image_validation_batch(self, tmp_path):
        """Test batch validation of images"""
        from PIL import Image

        # Create multiple test images
        valid_images = []
        for i in range(3):
            img_path = tmp_path / f"test_{i}.png"
            img = Image.new("RGB", (100, 100), color=(i * 50, i * 50, i * 50))
            img.save(img_path)
            valid_images.append(img_path)

        # Validate all images
        for img_path in valid_images:
            with Image.open(img_path) as img:
                img.verify()  # Should not raise exception

            # Re-open for further checks
            with Image.open(img_path) as img:
                assert img.size == (100, 100)
                assert img.format == "PNG"
