# import boto3
# import os
# import webvtt
# from urllib.parse import unquote_plus

# s3 = boto3.client("s3")
# translate = boto3.client("translate")

# TARGET_LANGUAGES = ["bn", "hi", "fr", "es"]  # Bengali, Hindi, French, Spanish


# def lambda_handler(event, context):

#     try:
#         file_key = unquote_plus(event["Records"][0]["s3"]["object"]["key"])
#         input_bucket = event["Records"][0]["s3"]["bucket"]["name"]

#         file_obj = s3.get_object(Bucket=input_bucket, Key=file_key)
#         vtt_content = file_obj["Body"].read().decode("utf-8")

#         captions = []
#         for caption in webvtt.read_buffer(vtt_content.splitlines()):
#             captions.append(
#                 {"start": caption.start, "end": caption.end, "text": caption.text}
#             )

#         file_name = os.path.basename(file_key).split(".")[0]
#         output_bucket = "movio-segments-subtitles-prod"
#         output_key_en = f"subtitles/{file_name}/lang_en.vtt"
#         s3.put_object(
#             Bucket=output_bucket,
#             Key=output_key_en,
#             Body=vtt_content.encode("utf-8"),
#             ContentType="text/vtt",
#         )

#         original_texts = [caption["text"] for caption in captions]
#         delimited_text = "<span>".join(original_texts)

#         for lang in TARGET_LANGUAGES:
#             translated_text = translate_text(delimited_text, lang)
#             translated_captions = translated_text.split("<span>")
#             new_vtt_content = create_translated_vtt(captions, translated_captions)

#             output_key = f"subtitles/{file_name}/lang_{lang}.vtt"

#             s3.put_object(
#                 Bucket=output_bucket,
#                 Key=output_key,
#                 Body=new_vtt_content.encode("utf-8"),
#                 ContentType="text/vtt",
#             )

#         s3.delete_object(Bucket=input_bucket, Key=file_key)

#         return {"statusCode": 200, "body": "Translation and upload successful"}

#     except Exception as e:
#         print(e)
#         return {"statusCode": 400, "body": f"Error in Execution: {str(e)}"}


# def translate_text(text, target_language):
#     """Translates the given text using aws Translate to the specified target language."""

#     response = translate.translate_text(
#         Text=text, SourceLanguageCode="en", TargetLanguageCode=target_language
#     )
#     return response["TranslatedText"]


# def create_translated_vtt(captions, translated_texts):
#     """Reconstruct a .vtt file with translated texts."""

#     vtt_output = "WEBVTT\n\n"
#     for i, caption in enumerate(captions):
#         vtt_output += f"{caption['start']} --> {caption['end']}\n"
#         vtt_output += f"{translated_texts[i]}\n\n"
#     return vtt_output
