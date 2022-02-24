# ==================================================================
# module list
# ------------------------------------------------------------------
# python        3.6    (apt)
# ==================================================================

FROM python:3.6.15-buster

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update
RUN apt-get install ffmpeg libsm6 libxext6 -y

# ===================Module Dependencies============================

RUN pip3 install cycler imageio kiwisolver matplotlib numpy opencv-python Pillow pyparsing python-dateutil scipy 

# ===================Copy Source Code===============================

RUN mkdir /module
WORKDIR /module

COPY src /module/src

####################################################################
######################## Append From Here Down #####################
####################################################################

# ===============bqapi for python3 Dependencies=====================
# pip install in this exact order
RUN pip3 install six
RUN pip3 install lxml
RUN pip3 install requests==2.18.4
RUN pip3 install requests-toolbelt

# =====================Build Directory Structure====================
# RUN mkdir /module
# WORKDIR /module

# COPY src /module/src
COPY PythonScriptWrapper.py /module/
COPY bqapi/ /module/bqapi
COPY EdgeDetection.xml /module/EdgeDetection.xml

ENV PATH /module:/module/src:$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
ENV PYTHONPATH $PYTHONPATH:/module/src
