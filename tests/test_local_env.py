import os

from packages.local_env import load_local_env_file


def test_load_local_env_file_strips_inline_comments(tmp_path, monkeypatch):
    env_path = tmp_path / '.env'
    env_path.write_text(
        'AUTO_FILE_SKIP_NEEDS_REVIEW=1 # temporary note\n'
        'QUOTED="keep # inside quotes"\n',
        encoding='utf-8',
    )

    monkeypatch.delenv('AUTO_FILE_SKIP_NEEDS_REVIEW', raising=False)
    monkeypatch.delenv('QUOTED', raising=False)

    load_local_env_file(env_path)

    assert os.environ['AUTO_FILE_SKIP_NEEDS_REVIEW'] == '1'
    assert os.environ['QUOTED'] == 'keep # inside quotes'
