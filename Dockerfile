FROM apache/airflow:3.1.7

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    xvfb \
    libxi6 \
    libgconf-2-4 \
    gnupg \
    curl \
    software-properties-common \
    \
    # libs chromedriver needs
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libxss1 \
    libxrandr2 \
    libasound2 \
    libxtst6 \
    libdrm2 \
    libx11-6 \
    libxcomposite1 \
    libxcursor1 \
    libxi6 \
    libxkbcommon0 \
    libxrender1 \
    libxext6 \
    fonts-liberation \
    fontconfig \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


# Add Google Chrome repository and install stable version
# Install a known stable Chrome version

RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    dpkg -i google-chrome-stable_current_amd64.deb || apt-get -f install -y && \
    rm google-chrome-stable_current_amd64.deb

# Install matching ChromeDriver version
RUN CHROMEDRIVER_VERSION=114.0.5735.90 && \
    wget -O /tmp/chromedriver.zip https://chromedriver.storage.googleapis.com/${CHROMEDRIVER_VERSION}/chromedriver_linux64.zip && \
    unzip /tmp/chromedriver.zip -d /usr/local/bin/ && \
    chmod +x /usr/local/bin/chromedriver && \
    rm /tmp/chromedriver.zip
        
RUN chmod +x /usr/local/bin/chromedriver

# RUN curl -fsSL https://dl.google.com/linux/linux_signing_key.pub | \
#     gpg --dearmor -o /etc/apt/trusted.gpg.d/google-chrome.gpg
# RUN echo "deb [arch=amd64 signed-by=/etc/apt/trusted.gpg.d/google-chrome.gpg] https://dl.google.com/linux/chrome/deb/ stable main" \
#     > /etc/apt/sources.list.d/google-chrome.list

# RUN apt-get update && apt-get install -y google-chrome-stable

RUN wget -q https://dl.google.com/linux/chrome/deb/pool/main/g/google-chrome-stable/google-chrome-stable_114.0.5735.198-1_amd64.deb -O /tmp/chrome-114.deb
RUN apt-get update
RUN apt-get install -y /tmp/chrome-114.deb
RUN apt-mark hold google-chrome-stable
RUN rm -f /tmp/chrome-114.deb


# Ensure Chrome is reachable as `google-chrome` in PATH

RUN ln -sf /usr/bin/google-chrome /usr/local/bin/chrome || true
# Switch back to airflow user
USER airflow

# Install Python packages
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt