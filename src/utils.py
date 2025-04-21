import smtplib
import configparser
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# === ConfigParser ===
config = configparser.ConfigParser()
config_path = "config/config.ini"
config.read(config_path)

def send_email(subject, body, to_email):
    from_email = config["email"]["from_email"]
    from_password = config["email"]["from_password"]
    # Set up the server
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(from_email, from_password)
    
    # Create the email
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = subject
    
    # Attach the body to the email
    msg.attach(MIMEText(body, 'plain'))
    
    # Send the email
    server.sendmail(from_email, to_email, msg.as_string())
    server.quit()