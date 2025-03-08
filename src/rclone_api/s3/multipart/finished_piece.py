import json
from dataclasses import dataclass

from rclone_api.types import EndOfStream


@dataclass
class FinishedPiece:
    part_number: int
    etag: str

    def to_json(self) -> dict:
        return {"part_number": self.part_number, "etag": self.etag}

    def to_json_str(self) -> str:
        return json.dumps(self.to_json(), indent=0)

    @staticmethod
    def to_json_array(parts: list["FinishedPiece | EndOfStream"]) -> list[dict]:
        non_none: list[FinishedPiece] = []
        for p in parts:
            if not isinstance(p, EndOfStream):
                non_none.append(p)
        non_none.sort(key=lambda x: x.part_number)
        # all_nones: list[None] = [None for p in parts if p is None]
        # assert len(all_nones) <= 1, "Only one None should be present"
        count_eos = 0
        for p in parts:
            if p is EndOfStream:
                count_eos += 1
        assert count_eos <= 1, "Only one EndOfStream should be present"
        return [p.to_json() for p in non_none]

    @staticmethod
    def from_json(json: dict | None) -> "FinishedPiece | EndOfStream":
        if json is None:
            return EndOfStream()
        return FinishedPiece(**json)
