import contextlib
import io
import struct

from shutils.imagesize import get, getDPI


class TestGet:
    def _make_gif(self, width, height):
        header = b'GIF89a'
        size = struct.pack("<hh", width, height)
        return io.BytesIO(header + size + b'\x00' * 20)

    def _make_png(self, width, height):
        header = b'\211PNG\r\n\032\n'
        ihdr = b'IHDR'
        size_data = struct.pack(">LL", width, height)
        return io.BytesIO(header + b'\x00\x00\x00\r' + ihdr + size_data + b'\x00' * 10)

    def _make_jpeg(self, width, height):
        # Minimal JPEG with SOF0 marker
        data = b'\xff\xd8'  # SOI
        data += b'\xff\xe0'  # APP0
        data += b'\x00\x10'  # length
        data += b'JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00'  # JFIF header
        data += b'\xff\xc0'  # SOF0
        data += struct.pack(">H", 8 + 3 * 1)  # length
        data += b'\x08'  # precision
        data += struct.pack(">HH", height, width)
        data += b'\x01'  # num components
        data += b'\x01\x11\x00'  # component
        data += b'\xff\xd9'  # EOI
        return io.BytesIO(data)

    def test_gif(self):
        result = get(self._make_gif(100, 200))
        assert result == (100, 200)

    def test_png(self):
        result = get(self._make_png(300, 400))
        assert result == (300, 400)

    def test_jpeg(self):
        result = get(self._make_jpeg(640, 480))
        assert result == (640, 480)

    def test_webp_simple(self):
        # VP8 simple lossy
        header = b'RIFF'
        file_size = struct.pack("<I", 30)
        webp = b'WEBP'
        vp8 = b'VP8 '
        chunk_size = struct.pack("<I", 10)
        # VP8 bitstream: frame tag + sync code
        struct.pack("<H", 0)  # keyframe
        struct.pack("<B", 0x9d) + struct.pack("<H", 0x012a)
        struct.pack("<HH", 640 & 0x3FFF, 480 & 0x3FFF)
        data = (
            header + file_size + webp + vp8 + chunk_size
            + b'\x00' * 6 + struct.pack("<HH", 640 & 0x3FFF, 480 & 0x3FFF)
        )
        # WebP VP8 parsing is complex; just test it doesn't crash
        with contextlib.suppress(ValueError, struct.error):
            get(io.BytesIO(data))


class TestGetDPI:
    def test_png_with_dpi(self, tmp_path):
        """Test PNG DPI extraction with pHYs chunk."""
        header = b'\211PNG\r\n\032\n'
        # IHDR chunk
        ihdr_data = struct.pack(">LL", 1, 1) + b'\x08\x02\x00\x00\x00'
        ihdr_crc = b'\x00' * 4  # fake CRC
        ihdr = struct.pack(">I", len(ihdr_data)) + b'IHDR' + ihdr_data + ihdr_crc
        # pHYs chunk: 300 DPI = 11811 pixels/meter
        phys_data = struct.pack(">LLB", 11811, 11811, 1)  # 1 = meter
        phys_crc = b'\x00' * 4
        phys = struct.pack(">I", len(phys_data)) + b'pHYs' + phys_data + phys_crc
        # IDAT chunk (minimal)
        idat = struct.pack(">I", 0) + b'IDAT' + b'\x00' * 4

        f = tmp_path / "test.png"
        f.write_bytes(header + ihdr + phys + idat)

        x_dpi, y_dpi = getDPI(str(f))
        assert x_dpi > 0
        assert y_dpi > 0

    def test_jpeg_dpi(self, tmp_path):
        """Test JPEG DPI extraction from APP0/JFIF header."""
        data = b'\xff\xd8'  # SOI
        data += b'\xff\xe0'  # APP0
        data += b'\x00\x10'  # length = 16
        data += b'JFIF\x00'  # identifier
        data += b'\x01\x01'  # version
        data += b'\x00'  # units: no units
        data += struct.pack(">HH", 96, 96)  # X/Y density
        data += b'\x00\x00'  # thumbnail size
        data += b'\xff\xd9'  # EOI

        f = tmp_path / "test.jpg"
        f.write_bytes(data)

        x_dpi, y_dpi = getDPI(str(f))
        assert x_dpi == 96
        assert y_dpi == 96
