from tributary.embedders.base import BaseEmbedder
import json


class BedrockEmbedder(BaseEmbedder):
    def __init__(self, model_name: str = "amazon.titan-embed-text-v2:0", region: str = "us-east-1"):
        super().__init__(model_name)
        self.region = region

    async def embed(self, texts: list[str]) -> list[list[float]]:
        import aiobotocore.session
        session = aiobotocore.session.get_session()
        async with session.create_client("bedrock-runtime", region_name=self.region) as client:
            vectors = []
            for text in texts:
                response = await client.invoke_model(
                    modelId=self.model_name,
                    body=json.dumps({"inputText": text}),
                    contentType="application/json",
                )
                body = json.loads(await response["body"].read())
                vectors.append(body["embedding"])
            return vectors
