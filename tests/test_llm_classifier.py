from packages.llm import classifier
from packages.llm.openai_client import OpenAIError


def test_classifier_marks_quota_failure_as_an_explicit_review_error(monkeypatch):
    monkeypatch.setattr(classifier, "llm_enabled", lambda: True)

    def fail(*_args, **_kwargs):
        raise OpenAIError("OpenAI API error: 429 insufficient_quota")

    monkeypatch.setattr(classifier, "call_openai_json", fail)

    decision = classifier.llm_classify_message("South lift out")

    assert decision["review_status"] == "error"
    assert decision["review_error"] == "insufficient_quota"
    assert decision["confidence"] == 0
    assert decision["needs_review"] is True


def test_classifier_marks_a_valid_model_response_completed(monkeypatch):
    monkeypatch.setattr(classifier, "llm_enabled", lambda: True)
    monkeypatch.setattr(
        classifier,
        "call_openai_json",
        lambda *_args, **_kwargs: {
            "is_issue": False,
            "signal_type": "discussion",
            "category": "other",
            "event_type": "non_issue",
            "confidence": 92,
        },
    )

    decision = classifier.llm_classify_message("Thanks")

    assert decision["review_status"] == "completed"
    assert "review_error" not in decision
