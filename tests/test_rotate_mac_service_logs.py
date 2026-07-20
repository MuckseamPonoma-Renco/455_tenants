import os

from scripts.rotate_mac_service_logs import rotate_log_directory, rotate_log_file


def test_rotate_log_file_keeps_inode_and_latest_complete_lines(tmp_path):
    log_path = tmp_path / "api.launchd.err.log"
    log_path.write_bytes(b"discard\n" * 20 + b"retain-one\nretain-two\n")
    before_inode = log_path.stat().st_ino

    result = rotate_log_file(log_path, max_bytes=64, retain_bytes=48)

    assert result is not None
    assert result["before_bytes"] > result["after_bytes"]
    assert log_path.stat().st_ino == before_inode
    assert log_path.read_bytes().endswith(b"retain-one\nretain-two\n")


def test_rotate_log_directory_skips_small_files_and_symlinks(tmp_path):
    small = tmp_path / "small.out.log"
    small.write_text("small\n", encoding="utf-8")
    large = tmp_path / "large.out.log"
    large.write_bytes(b"old\n" * 30 + b"latest\n")
    linked = tmp_path / "linked.out.log"
    os.symlink(large, linked)

    rotated = rotate_log_directory(tmp_path, max_bytes=64, retain_bytes=32)

    assert [item["path"] for item in rotated] == ["large.out.log"]
    assert small.read_text(encoding="utf-8") == "small\n"
    assert large.read_bytes().endswith(b"latest\n")
