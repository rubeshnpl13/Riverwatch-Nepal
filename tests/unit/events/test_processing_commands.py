from pathlib import Path

from riverwatch.events import (
    processing_commands,
)


class FakeSpark:
    def stop(self) -> None:
        pass


def test_processing_worker_command_accepts_events_root(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    events_root = (
        tmp_path
        / "events"
    )

    lake_root = (
        tmp_path
        / "lake"
    )

    monkeypatch.setattr(
        processing_commands,
        "create_local_spark_session",
        lambda **_: FakeSpark(),
    )

    result = (
        processing_commands
        .processing_worker_main(
            [
                "--events-root",
                str(events_root),
                "--lake-root",
                str(lake_root),
                "--once",
            ]
        )
    )

    assert result == 0

    output = (
        capsys.readouterr()
        .out
    )

    assert (
        "Processing worker cycle complete"
        in output
    )

    assert (
        "processed=0"
        in output
    )