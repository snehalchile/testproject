import smtplib
from email.mime.text import MIMEText
from utils.config_loader import load_config
from email.mime.multipart import MIMEMultipart
config = load_config()

def send_email(recipient, subject, body):
    """Simulate sending an email."""
    print(f"Sending email to: {recipient}")
    print(f"Subject: {subject}")
    print("Body:")
    print(body)
    
    #Todo: Please uncomment below section for email testing
    """Send an email using SMTP."""
    # sender_email = config['sender_email']  
    # password = config['password']         
    # # recipient_email = config['recipient_email']
    
    # # Create a MIMEMultipart email object
    # msg = MIMEMultipart()
    # msg['From'] = sender_email
    # msg['To'] = recipient
    # msg['Subject'] = subject
    
    # # Attach the body text
    # msg.attach(MIMEText(body, 'plain'))

    # # Connect to the SMTP server and send the email
    # try:
    #     with smtplib.SMTP('smtp.gmail.com', 587) as server:  
    #         server.starttls()  
    #         server.login(sender_email, password)  
    #         server.sendmail(sender_email, recipient, msg.as_string())  
    #         print(f"Email sent successfully to {recipient}")
    # except Exception as e:
    #     print(f"Failed to send email: {e}")

def email_error_log(recipient, error_file='error_log.txt'):
    """Send the error log via email."""
    with open(error_file, mode='r') as file:
        body = file.read()
    
    subject = "Error Log"
    send_email(recipient, subject, body)

