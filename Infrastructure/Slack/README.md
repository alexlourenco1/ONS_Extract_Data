# Slack Library

This projects is an implementation of `slack-client`, official Slack library, of main functions as send a message
or upload a file to a slack channel, for example.
 
# Setup

First of all, the dependencies listed on [requirements.txt](requirements.txt) file must be installed, as example bellow:

```
pip install slackclient
pip install python-dotenv
```

Finally, the following environment variables must be declared:
  
* **SLACK_API_TOKEN**: Slack token to authenticate on workspace.
* **SLACK_DEFAULT_ICON**: URL of an image that is used by application as user image.

  
## Usage

To use this project you just need import the class `Slack`, included in the main module [Slack.py](Slack.py),
instantiate it and call the functions with the indicated parameters, as example bellow:

```Python
from Infrastructure.Slack import Slack

slack = Slack()

slack.send_message(channel_name='channel name',
                   message='Text Message',
                   username='username')

slack.upload_file(channel_name='Research',
                  file_path='C:\\Downloads\\example.pdf',
                  comment='File comments',
                  title='Spotlight Title',
                  file_name='file_name',
                  file_type='pdf')
```
