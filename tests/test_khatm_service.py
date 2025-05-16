import pytest
from bot.services.khatm_service import process_khatm_number

def test_process_khatm_number_salavat(db_connection, sample_group, sample_topic):
    previous_total, new_total, completed = process_khatm_number(
        group_id=sample_group,
        topic_id=sample_topic,
        number=50,
        khatm_type="salavat",
        current_value=100,
        db_conn=db_connection
    )
    assert previous_total == 150
    assert new_total == 150
    assert not completed

    cursor = db_connection.cursor()
    cursor.execute("SELECT current_total FROM topics WHERE topic_id = ? AND group_id = ?",
                   (sample_topic, sample_group))
    assert cursor.fetchone()["current_total"] == 150

def test_process_khatm_number_ghoran(db_connection, sample_group, sample_topic):
    previous_total, new_total, completed = process_khatm_number(
        group_id=sample_group,
        topic_id=sample_topic,
        number=10,
        khatm_type="ghoran",
        current_value=100,
        db_conn=db_connection
    )
    assert previous_total == 110
    assert new_total == 110
    assert not completed

    cursor = db_connection.cursor()
    cursor.execute("SELECT current_verse FROM topics WHERE topic_id = ? AND group_id = ?",
                   (sample_topic, sample_group))
    assert cursor.fetchone()["current_verse"] == 110