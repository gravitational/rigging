FROM quay.io/gravitational/debian-grande:stretch

ADD ./build/rig /usr/local/bin/rig
RUN chmod +x /usr/local/bin/rig
