from datetime import datetime
import os
import slack


class Slack(object):

    __slack_token: str
    __slack_default_icon: str
    __client: slack.WebClient

    def __init__(self, token: str = None, icon_url: str = None):
        """
        Implementation of Slack Client
        :param token: token to connect with client
        :param icon_url: Url of icon
        """
        self.__slack_token = os.environ['SLACK_API_TOKEN'] if 'SLACK_API_TOKEN' in os.environ else token
        self.__slack_default_icon = os.environ['SLACK_DEFAULT_ICON'] if 'SLACK_DEFAULT_ICON' in os.environ else icon_url

        assert self.__slack_token, 'Slack Token must be informed!'

        self.__client = slack.WebClient(token=self.__slack_token)

    def list_channels(self) -> dict:
        return self.__client.conversations_list(types="public_channel, private_channel").data['channels']

    def get_channel_id(self, channel_name: str) -> str:
        return next(channel['id'] for channel in self.list_channels() if channel['name'] == channel_name)

    def get_channel_info(self, channel_name: str) -> dict:

        assert channel_name

        return self.__client.channels_info(channel=self.get_channel_id(channel_name)).data['channel']

    def send_message(self, channel_name: str, message: str = None, username: str = None, struct_message: list = None):

        assert channel_name

        channel_id = self.get_channel_id(channel_name=channel_name)

        self.__client.chat_postMessage(
            channel=channel_id,
            text=message,
            username=username,
            blocks=struct_message,
            icon_url=self.__slack_default_icon
        )

    def read_message(self, channel_name: str, oldest: datetime = None, latest: datetime = datetime.now(), limit: int = 100) -> list:

        assert channel_name

        channel_id = self.get_channel_id(channel_name=channel_name)

        response = self.__client.conversations_history(
            channel=channel_id,
            oldest=str(oldest.timestamp()) if oldest else 0,
            latest=str(latest.timestamp()),
            count=limit
        )
        return response['messages']

    def delete_message(self, channel_name: str, ts: str):

        assert channel_name

        channel_id = self.get_channel_id(channel_name=channel_name)

        self.__client.chat_delete(channel=channel_id, ts=ts)

    def upload_file(self, channel_name: str, file_path: str, comment: str, title: str, file_name: str, file_type: str):

        assert channel_name

        channel_id = self.get_channel_id(channel_name=channel_name)

        with open(file_path, 'rb') as tmp_file:
            self.__client.files_upload(
                filename=file_name,
                filetype=file_type,
                initial_comment=comment,
                channels=channel_id,
                file=tmp_file,
                title=title
            )
