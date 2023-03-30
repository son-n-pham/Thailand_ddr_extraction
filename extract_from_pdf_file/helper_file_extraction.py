from PyPDF2 import PdfReader


def all_text_in_pdf(pdf_file):
    """Extract all text from pdf_file

    Args:
                    pdf_file (string): PDF file name

    Returns:
                    string: all text in pdf_file
    """
    reader = PdfReader(pdf_file)
    text = ""
    for page in reader.pages:
        text += page.extract_text() + "\n"
    return text
