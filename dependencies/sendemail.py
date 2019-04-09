import smtplib
import ssl


def send_email(e):
    application_name = "my_spark_etl"
    port = 587  # For starttls
    smtp_server = "smtp.gmail.com"

    # Test Account credetials , replace with respective email id at the time of testing
    sender_email = "naveenkumarsmtp@gmail.com"
    receiver_email = "mndlsoft@gmail.com"
    password = "L@gError"

    message = f"""\
    Subject: Application {application_name} Failed!!!

    Application {application_name} failed with : {e}"""

    context = ssl.create_default_context()
    with smtplib.SMTP(smtp_server, port) as server:
        server.ehlo()  # Can be omitted
        server.starttls(context=context)
        server.ehlo()  # Can be omitted
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message)
