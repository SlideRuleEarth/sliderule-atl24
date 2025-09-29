FROM amazonlinux:2023

# runtime dependencies
RUN dnf update -y \
    && dnf install -y \
        wget \
        git \
    && dnf clean all \
    && rm -rf /var/cache/yum

# install mamba for python based algorithms
RUN wget https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-aarch64.sh \
     && bash Miniforge3-Linux-aarch64.sh -b

# install python dependencies for python based algorithms
COPY conda-linux-aarch64.lock .
RUN /root/miniforge3/bin/mamba create --copy -p /env --file conda-linux-aarch64.lock \
    && /root/miniforge3/bin/conda clean -afy

# install h5coro
RUN /env/bin/pip install git+https://github.com/SlideRuleEarth/h5coro.git

# install python applications
COPY runner.py /runner.py

# container defaults
SHELL ["/bin/bash", "-c"]
CMD ["/env/bin/python", "runner.py"]