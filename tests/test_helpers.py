import pytest
from bot.utils.helpers import parse_number, format_khatm_message

def test_parse_number_english():
    assert parse_number("123") == 123
    assert parse_number("123.45") == 123.45

def test_parse_number_persian():
    assert parse_number("۱۲۳") == 123
    assert parse_number("۱۲۳٫۴۵") == 123.45

def test_parse_number_invalid():
    assert parse_number("abc") is None
    assert parse_number("") is None

def test_format_khatm_message_salavat():
    message = format_khatm_message(
        khatm_type="salavat",
        previous_total=100,
        number=50,
        new_total=150,
        sepas_text="یا علی"
    )
    expected = "از 100 صلوات، 50 صلوات فرستاده شد.\nجمع: 150\nیا علی"
    assert message == expected

def test_format_khatm_message_ghoran():
    message = format_khatm_message(
        khatm_type="ghoran",
        previous_total=100,
        number=10,
        new_total=110,
        sepas_text="الحمدلله"
    )
    expected = "از آیه 100، 10 آیه خوانده شد.\nآیه بعدی: 111\nالحمدلله"
    assert message == expected

def test_format_khatm_message_zekr():
    message = format_khatm_message(
        khatm_type="zekr",
        previous_total=200,
        number=30,
        new_total=230,
        sepas_text="ممنون",
        zekr_text="سبحان الله"
    )
    expected = "از 200 سبحان الله، 30 سبحان الله گفته شد.\nجمع: 230\nممنون"
    assert message == expected