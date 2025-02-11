import requests
import smtplib
import time
import logging
from tenacity import retry, wait_fixed, stop_after_attempt
from signal import signal, SIGINT, SIGTERM

# Setup logging
logging.basicConfig(
    filename="monitor.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger()

# Email configuration
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_USER = "mikael.hinton19@gmail.com"
EMAIL_PASS = "fmtv agii evmd qxmn"  # App-specific password
TO_EMAIL = ["mikael.hinton19@gmail.com", "presleywright@icloud.com"]

# Webpage to monitor
URL = "https://www.youshouldknowstudios.com/"
CHECK_STRING = "NEW MERCH COMING SOON..."

# Counter for sent emails
email_count = 0
MAX_EMAILS = 3
CHECK_INTERVAL = 10  # Time between checks in seconds

# Graceful exit flag
keep_running = True


def graceful_exit(signum, frame):
    global keep_running
    logger.info("Received termination signal, shutting down...")
    keep_running = False


signal(SIGINT, graceful_exit)
signal(SIGTERM, graceful_exit)


def send_email(subject, body):
    global email_count
    try:
        logger.info(f"Preparing to send email to {len(TO_EMAIL)} recipients...")
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_USER, EMAIL_PASS)
            message = f"Subject: {subject}\n\n{body}"
            for recipient in TO_EMAIL:
                server.sendmail(EMAIL_USER, recipient, message)
                logger.info(f"Email sent to {recipient}")
            email_count += 1
            logger.info(f"Email sent successfully! Total emails sent: {email_count}")
    except Exception as e:
        logger.error(f"Error sending email: {e}")


@retry(wait=wait_fixed(10), stop=stop_after_attempt(3))
def fetch_webpage():
    logger.info(f"Checking the webpage: {URL}...")
    response = requests.get(URL, timeout=10)
    response.raise_for_status()
    return response


def monitor_webpage():
    try:
        response = fetch_webpage()
        if CHECK_STRING not in response.text:
            message = (
                "The webpage has changed from 'NEW MERCH COMING SOON...!' "
                "Website: https://www.youshouldknowstudios.com/"
            )
            logger.info(message)
            send_email("YouShouldKnowPodcast MERCH", message)
        else:
            logger.info(f"Check string '{CHECK_STRING}' still found on the webpage.")
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error while monitoring webpage: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


if __name__ == "__main__":
    logger.info("Starting the monitoring process...")
    while email_count < MAX_EMAILS and keep_running:
        monitor_webpage()
        if email_count >= MAX_EMAILS:
            logger.info(f"Sent {email_count} emails, exiting the program.")
            break
        time.sleep(CHECK_INTERVAL)
    logger.info("Monitoring process finished.")
