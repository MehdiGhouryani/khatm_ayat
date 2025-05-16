import pytest
from bot.services.stats_service import get_group_stats, get_ranking

def test_get_group_stats(db_connection, sample_group, sample_topic, sample_user):
    stats = get_group_stats(sample_group, sample_topic)
    assert stats == {
        "total_salavat": 100,
        "total_zekr": 0,
        "total_ayat": 0
    }

def test_get_ranking(db_connection, sample_group, sample_topic, sample_user):
    rankings = get_ranking(sample_group, sample_topic)
    assert len(rankings) == 1
    assert rankings[0]["user_id"] == 12345
    assert rankings[0]["total_salavat"] == 100