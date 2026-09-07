from clients.models import ClientError
from top_of_book_bot import is_execution_channel_quiesced_error


def test_broker_quiesce_write_is_classified_as_expected_shutdown_race():
    assert is_execution_channel_quiesced_error(
        ClientError("execution channel worker-00 is quiesced")
    )
    assert not is_execution_channel_quiesced_error(ClientError("order rejected"))
    assert not is_execution_channel_quiesced_error(
        ClientError("execution channel worker-00 is unavailable")
    )
