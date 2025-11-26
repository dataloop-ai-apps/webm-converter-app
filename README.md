# **WEBM Converters**

A Dataloop application designed to convert video files into the WEBM format, an open, royalty-free media format optimized for web use.

---

## **Table of Contents**

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Installation & Setup](#installation--setup)
- [Usage](#usage)
- [How It Works](#how-it-works)
- [Contributing](#contributing)
- [Troubleshooting](#troubleshooting)

---

## **Overview**

The `WEBM Converter` app allows users to seamlessly convert videos to WEBM format directly within the Dataloop environment. It supports conversion using two methods: `OpenCV` or `FFmpeg`, providing flexibility according to project requirements.

---

## **Prerequisites**

- **Python 3.7 or higher**
- **Dataloop Python SDK** ([Installation Guide](https://github.com/dataloop-ai/dtlpy))
- **Dataloop CLI** ([CLI Documentation](https://sdk-docs.dataloop.ai/en/latest/cli.html))
- **Git**

---

## **Installation & Setup**

Clone the repository:

```bash
git clone https://github.com/dataloop-ai-apps/webm_converter.git
cd webm_converter
```

Install necessary dependencies:

```bash
pip install -r requirements.txt
```

---

## **Usage**

### **Deploying the App**

Deploy the app service by running:

```bash
python deploy_webm.py
```

This sets up the app within your Dataloop project environment.

---

## **How It Works**

The application workflow is managed by the following methods:

- **`run()`**: Main function that initiates file downloading and conversion.
- **`webm_converter()`**: Handles the selection of the conversion method.
- **`_convert_to_webm_opencv()`**: Converts video using OpenCV.
- **`_convert_to_webm_ffmpeg()`**: Converts video using FFmpeg.
- **`verify_webm_conversion()`**: Verifies if the converted video is valid.
- **`validate_video()`**: Validates the integrity and metadata of the video file.

The workflow involves:
1. Downloading the video file.
2. Converting the file using the selected method.
3. Validating and verifying the converted video.
4. Uploading the converted file back to the Dataloop platform.

---

## **Contributing**

Contributions are warmly welcomed! To report bugs, request features, or contribute code improvements, please open an issue or create a pull request.

---

## **Troubleshooting**

- **Conversion issues:**
  - Ensure that dependencies (FFmpeg, OpenCV) are correctly installed.
- **Deployment issues:**
  - Check your Dataloop SDK and CLI configurations.

---
