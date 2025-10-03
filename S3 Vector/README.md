# 📂 S3 Vector Storage with Azure OpenAI Embeddings

This project allows you to **generate embeddings for text using Azure OpenAI** and **store them in an S3 Vector Index**. It is structured for scalability, modularity, and production readiness.

---

## 🚀 Features

- Generate text embeddings using **Azure OpenAI**.
- Store vectors in **AWS S3 Vector Index**.
- Config management using **Pydantic Settings** (`.env` support).
- Modular structure with logging and exception handling.
- Ready for **unit testing** and CI/CD pipelines.

---

## 🗂 Project Structure

S3 Vectors/
│── src/
│ ├── config.py # Config management using Pydantic
│ ├── services/
│ │ ├── azure_embedding_service.py
│ │ ├── s3_vector_service.py
│ └── main.py # Example usage
│
│── tests/ # Unit tests (optional)
│── .env # Environment variables (not committed)
│── requirements.txt
│── README.md
│── .gitignore


---

## ⚙️ Installation

1. Clone the repository:

```bash
git clone https://github.com/yourusername/s3-vector-embedding.git
cd s3-vector-embedding

2. Create a virtual environment and activate it:
python -m venv venv
source venv/bin/activate  # Linux/macOS
venv\Scripts\activate     # Windows

3. Install dependencies:

pip install -r requirements.txt

📝 Environment Variables

Create a .env file in the project root:

# Azure OpenAI
API_KEY=your_azure_api_key
API_VERSION=2024-05-01-preview
ENDPOINT=https://your-endpoint.openai.azure.com/
EMBEDDING_MODEL=text-embedding-ada-002

# AWS / S3 Vectors
aws_user_access_key=your_aws_access_key
aws_user_secret_key=your_aws_secret_key
s3_region=us-east-1
s3_bucket=your-s3-vector-bucket
s3_vector_index=your-vector-index


