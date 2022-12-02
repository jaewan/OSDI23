import ray
from models import *
from datasets import load_dataset


@ray.remote
def get_image(dataset):
    '''
        image = dataset["test"]["image"][0]

        url = 'http://images.cocodataset.org/val2017/000000039769.jpg'
        image = Image.open(requests.get(url, stream=True).raw)
    '''
    image = 1
    img = ray.put(image)
    return img



if __name__ == '__main__':
    import argparse

    dataset = load_dataset("huggingface/cats-image")
    quit()

    MODEL_COUNT=len(models.MODELS)


    img_models = []
    img_models.append(Resnet18())
    img_models.append(Resnet50())
    img_models.append(Resnet101())
    img_models.append(BEiT())
    img_models.append(ConvNeXT())
    img_models.append(ViT384())

    print(img_models[-1].model_name)
    print(img_models[-1].eval('hi'))
