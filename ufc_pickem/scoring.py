from dataclasses import dataclass


@dataclass
class PickScore:
    winner_points: int
    method_points: int
    round_points: int

    @property
    def total_points(self) -> int:
        return (
            self.winner_points
            + self.method_points
            + self.round_points
        )


def normalize_text(value: str | None) -> str:
    if value is None:
        return ""

    return value.strip().lower()


def calculate_pick_score(
    predicted_winner: str,
    predicted_method: str | None,
    predicted_round: int | None,
    official_winner: str,
    official_method: str | None,
    official_round: int | None,
) -> PickScore:
    winner_is_correct = (
        normalize_text(predicted_winner)
        == normalize_text(official_winner)
    )

    if not winner_is_correct:
        return PickScore(
            winner_points=0,
            method_points=0,
            round_points=0,
        )

    method_is_correct = (
        predicted_method is not None
        and official_method is not None
        and normalize_text(predicted_method)
        == normalize_text(official_method)
    )

    round_is_correct = (
        predicted_round is not None
        and official_round is not None
        and predicted_round == official_round
    )

    return PickScore(
        winner_points=1,
        method_points=1 if method_is_correct else 0,
        round_points=1 if round_is_correct else 0,
    )


if __name__ == "__main__":
    perfect_pick = calculate_pick_score(
        predicted_winner="Max Holloway",
        predicted_method="KO/TKO",
        predicted_round=2,
        official_winner="Max Holloway",
        official_method="KO/TKO",
        official_round=2,
    )

    correct_winner_only = calculate_pick_score(
        predicted_winner="Max Holloway",
        predicted_method="Decision",
        predicted_round=5,
        official_winner="Max Holloway",
        official_method="KO/TKO",
        official_round=2,
    )

    incorrect_winner = calculate_pick_score(
        predicted_winner="Justin Gaethje",
        predicted_method="Decision",
        predicted_round=3,
        official_winner="Max Holloway",
        official_method="KO/TKO",
        official_round=2,
    )

    print(f"Perfect pick: {perfect_pick.total_points} points")
    print(
        "Correct winner only: "
        f"{correct_winner_only.total_points} point"
    )
    print(f"Incorrect winner: {incorrect_winner.total_points} points")