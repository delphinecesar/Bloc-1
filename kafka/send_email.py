
import smtplib
from email.mime.text import MIMEText

class Email():
    def __init__(self, name):
        self.name = name

    # Function: send email
    def send_email(recipient, subject, body):
        smtp_host = 'smtp.gmail.com'
        smtp_port = 587
        smtp_username = 'mygepetochat@gmail.com'
        smtp_password = '$PASSWORD'

        # message
        message = MIMEText(body)
        message['From'] = smtp_username
        message['To'] = recipient
        message['Subject'] = subject

        # Connection
        server = smtplib.SMTP(smtp_host, smtp_port)
        server.starttls()
        server.login(smtp_username, smtp_password)

        # Send email
        server.sendmail(smtp_username, recipient, message.as_string())

        # Quit the STMP connection
        server.quit()

    # Send email function
    def send_email_if_fraud(dataset):
        for trans_num, row in dataset.iterrows():
            prediction = row['prediction']
            if prediction == 1:
                recipient = 'delphinecesar@gmail.com'
                subject = 'Attention: fraud detected'
                body = 'A transaction has been detected as fraudulent, please check transaction number: ' + str(row['trans_num'])
                Email.send_email(recipient, subject, body)