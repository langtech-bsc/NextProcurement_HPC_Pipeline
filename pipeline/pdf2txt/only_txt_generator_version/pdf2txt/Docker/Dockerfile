FROM ubuntu:22.04


WORKDIR /app



RUN apt-get update && apt-get install -y software-properties-common 

RUN add-apt-repository ppa:deadsnakes/ppa -y && apt-get update && apt-get install -y python3.10 python3-pip 
RUN  python3 -m pip install --upgrade  pip  --no-cache-dir

RUN apt-get update && apt-get install -y python-is-python3 git git-lfs wget cmake libfreetype6-dev libfontconfig-dev libnss3-dev libjpeg-dev libopenjp2-7 libopenjp2-7-dev ffmpeg libsm6 libxext6 libleptonica-dev tesseract-ocr libtesseract-dev




RUN git clone https://gitlab.freedesktop.org/poppler/poppler.git
# git checkout poppler-0.89.0
WORKDIR /app/poppler 
RUN mkdir build 
WORKDIR /app/poppler/build 
RUN cmake -DCMAKE_BUILD_TYPE=Release  -DCMAKE_INSTALL_PREFIX:PATH=/usr/local     -DENABLE_UNSTABLE_API_ABI_HEADERS=ON     -DBUILD_GTK_TESTS=OFF     -DBUILD_QT5_TESTS=OFF     -DBUILD_CPP_TESTS=OFF     -DENABLE_CPP=ON     -DENABLE_GLIB=OFF     -DENABLE_GOBJECT_INTROSPECTION=OFF     -DENABLE_GTK_DOC=OFF     -DENABLE_QT5=OFF     -DBUILD_SHARED_LIBS=ON  -DENABLE_GPGME=OFF  -DENABLE_LIBTIFF=OFF  -DENABLE_QT6=OFF -DENABLE_LCMS=OFF -DENABLE_LCMS=OFF  -DENABLE_LIBCURL=OFF -DENABLE_BOOST=OFF ..
RUN make 
RUN make install
WORKDIR /app




RUN wget  https://github.com/tesseract-ocr/tessdata/raw/main/spa.traineddata -P /usr/share/tesseract-ocr/4.00/tessdata 
RUN wget  https://github.com/tesseract-ocr/tessdata/raw/main/cat.traineddata -P /usr/share/tesseract-ocr/4.00/tessdata
RUN wget  https://github.com/tesseract-ocr/tessdata/raw/main/eus.traineddata -P /usr/share/tesseract-ocr/4.00/tessdata
RUN wget  https://github.com/tesseract-ocr/tessdata/raw/main/glg.traineddata -P /usr/share/tesseract-ocr/4.00/tessdata

ENV TESSDATA_PREFIX /usr/share/tesseract-ocr/4.00/tessdata/




# pdf2txt installation

RUN git clone --depth 1 --branch main https://github.com/nextprocurement/pdf2txt.git
WORKDIR /app/pdf2txt
RUN python3 -m pip install --use-pep517  --no-cache-dir -r requirements.txt

ENV PKG_CONFIG_PATH /usr/local/lib/pkgconfig
ENV LD_LIBRARY_PATH /usr/local/lib:$LD_LIBRARY_PATH

# Model downloading
WORKDIR /app/pdf2txt/pipeline/models/nextprocurement_pdfutils/
RUN git clone --depth 1 https://huggingface.co/BSC-LT/NextProcurement_pdfutils /app/pdf2txt/pipeline/models/nextprocurement_pdfutils/




# Cleaning

RUN rm -rf /app/poppler/ && rm -rf /var/cache/apt/archives/



# APP on container
WORKDIR /app/pdf2txt
#CMD python pipeline/pipeline.py
CMD /bin/bash