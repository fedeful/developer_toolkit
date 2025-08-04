FROM python:3.11.9

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Update and install necessary packages
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install pipx
RUN python -m pip install --upgrade pip \
    && python -m pip install pipx \
    && pipx ensurepath

# Optional: add pipx to PATH (for all users)
ENV PATH=/root/.local/bin:$PATH

RUN pipx install poetry \
    # install an old version of poetry for mp 1.8
    && pipx install --suffix "_1.5" poetry==1.5.1 \
    && pipx ensurepath

RUN poetry completions bash >> ~/.bash_completion
RUN poetry config repositories.prometeia "https://testproject-modeling-platform.northeurope.cloudapp.azure.com/nexus/repository/prometeia/"
RUN poetry config http-basic.prometeia "federicof" "qmphAuSZQti4JL"
