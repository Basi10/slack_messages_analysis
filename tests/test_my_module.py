from src.loader import SlackDataLoader

def test_add():
    test_data = SlackDataLoader('tests/')
    extracted_data = test_data.slack_parser('tests/test_files/')
    assert extracted_data.shape == (102, 11)
    assert extracted_data['sender_name'].nunique() == 4
    assert extracted_data['reply_count'].max() == 0