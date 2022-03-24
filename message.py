import email
import warnings
import pandas as pd
from bs4 import BeautifulSoup

def parse_body(message):

    body = ''

    if message.is_multipart():
        for part in message.walk():
            content_type = part.get_content_type()
            content_disposition = str(part.get('Content-Disposition'))

            if (content_type in ['text/html', 'text/txt'] and
                'attachment' not in content_disposition):
                body = part.get_payload(decode=True)
                break
    else:
        body = message.get_payload(decode=True)

    return BeautifulSoup(body, 'html5lib').get_text()


def stream_trec07p(dataset_path):

    warnings.filterwarnings('ignore', category=UserWarning, module='bs4')

    with open(f'{dataset_path}/full/index') as full_index:
        for row in full_index:
            label, filepath = row.split()
            ix = filepath.split('.')[-1]

            with open(f'{dataset_path}/data/inmail.{ix}', 'rb') as email_file:
                message = email.message_from_binary_file(email_file)
                yield (
                    parse_body(message),
                    label
                )


if __name__ == '__main__':
    dataset_path = 'trec07p'
    parsed_emails = [parsed_email for parsed_email in stream_trec07p(dataset_path)]

    columns = ['body', 'y']
    df = pd.DataFrame(parsed_emails, columns=columns)

    df.to_csv('trec07p.csv', index=False)