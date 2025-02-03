# Use Python base image
FROM python:3.9  

# Set working directory
WORKDIR /app  

# Copy script files
COPY . .  
COPY game_genres.csv /app/

# Install dependencies
RUN pip3 install --no-cache-dir -r requirements.txt  

# Install cron and ensure cron log file exists
RUN apt-get update && apt-get install -y cron  
RUN touch /var/log/cron.log  # Ensure log file exists

# Copy crontab file and set permissions
COPY cronjob /etc/cron.d/cronjob  
RUN chmod 0644 /etc/cron.d/cronjob  
RUN crontab /etc/cron.d/cronjob  

# Ensure cron job writes to the log file
CMD cron && tail -f /var/log/cron.log
