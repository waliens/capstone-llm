FROM gitpod/workspace-python:latest

RUN pyenv install 3.11 \
    && pyenv global 3.11

RUN wget "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -O "awscliv2.zip" && \
    unzip awscliv2.zip && \
    rm -rf awscliv2.zip && \
    sudo ./aws/install --install-dir /opt/aws-cli --bin-dir /usr/local/bin/ && \
    sudo chmod a+x /opt/

RUN curl -sSL https://install.python-poetry.org | python3 -