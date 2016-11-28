FROM quay.io/gravitational/debian-grande:0.0.1

ADD ./build/rig /usr/local/bin/rig
RUN chmod +x /usr/local/bin/rig
