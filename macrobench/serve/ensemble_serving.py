import ray
from models import *
from datasets import load_dataset
import random
import numpy as np

params=0
dataset = 0
def get_params():  
    import argparse 
    global params
    parser = argparse.ArgumentParser()
    parser.add_argument('--NUM_BATCHES', '-nb', type=int, default=100)
    parser.add_argument('--BATCH_SIZE', '-bs', type=int, default=1)
    parser.add_argument('--BATCH_INTERVAL', '-bi', type=int, default=1)
    parser.add_argument('--RESULT_PATH', '-r', type=str, default="../data/dummy.csv")
    parser.add_argument('--OBJECT_STORE_SIZE', '-o', type=int, default=1_000_000_000)
    parser.add_argument('--MAX_MODEL_RUN', '-m', type=int, default=0)
    args = parser.parse_args()
    params = vars(args) 

@ray.remote
def get_image():
    from PIL import Image
    idx = random.randint(0, 7)
    if idx == 0:
        image = Image.open(r'data/korean-flag.jpg')
    elif idx == 1:
        image = Image.open(r'data/Double-Cat-Wallpaper.jpg')
    elif idx == 2:
        image = Image.open(r'data/qVnC9UJ.jpg')
    elif idx == 3:
        image = dataset["test"]["image"][0]
    elif idx == 4:
        image = Image.open(r'data/104801ab.jpg')
    elif idx == 5:
        image = Image.open(r'data/219648fg.jpg')
    elif idx == 6:
        image = Image.open(r'data/400853mt.jpg')
    elif idx == 7:
        image = Image.open(r'data/911093ab.jpg')
    return image

@ray.remote
def preprocess(img):
    import torchvision.transforms as transforms
    normalize = transforms.Normalize(mean=[0.485, 0.456, 0.406],
                                     std=[0.229, 0.224, 0.225])
    '''
    preprocessing = transforms.Compose([
       transforms.RandomSizedCrop(224),
       transforms.RandomHorizontalFlip(),
       transforms.ToTensor(),
       normalize,
    ])
    '''
    preprocessing = transforms.Compose([
        transforms.Resize(256),
        transforms.CenterCrop(224),
        transforms.ToTensor(),
        normalize
    ])
    return preprocessing(img)
                                                                                                         
def initialize():
    import os
    import json
    get_params()

    # Initialize Ray to spill to local disk
    spill_dir = os.getenv('RAY_SPILL_DIR')
    if spill_dir:
        ray.init(_system_config={"object_spilling_config": json.dumps({"type": "filesystem",
                                    "params": {"directory_path": spill_dir}},)}, object_store_memory=params['OBJECT_STORE_SIZE'])
        print("Ray spill dir set")
    else:
        ray.init(object_store_memory=params['OBJECT_STORE_SIZE'])
        print("Ray default init")

    # Load image libraries
    random.seed(0)
    global dataset
    dataset = load_dataset("huggingface/cats-image")

def get_arbitrary_model():
    #return img_models[random.randint(0, num_models)]
    return img_models[random.randint(0, len(img_models) - 1)]


@ray.remote(num_cpus=1)
def aggregator(img, seq):
    random.seed(seq)
    INITIAL_MODEL_BATCH = 3
    processed_img = preprocess.remote(img)

    prediction_ref = []
    predictions = {}
    for _ in range(INITIAL_MODEL_BATCH):
        m = get_arbitrary_model()
        if _ == 0:
            prediction_ref.append(m.predict.remote(processed_img))
        else:
            prediction_ref.append(m.predict.remote(img))
    num_models_run = INITIAL_MODEL_BATCH

    for i in range(INITIAL_MODEL_BATCH):
        pred = ray.get(prediction_ref[i])
        predictions[pred] = predictions.get(pred,0) + 1
        if predictions[pred] >= ((num_models_run//2) + 1):
            return num_models_run

    vote = 0
    while vote < ((num_models_run//2) + 1) and num_models_run <= params['MAX_MODEL_RUN']:
        m = get_arbitrary_model()
        original_image = random.randint(0, params['MAX_MODEL_RUN']//2)
        pred = 0
        if original_image:
            pred = ray.get(m.predict.remote(img))
        else:
            pred = ray.get(m.predict.remote(processed_img))

        num_models_run += 1
        predictions[pred] = predictions.get(pred,0) + 1
        vote = predictions[pred]

    return num_models_run


def batch_submitter():
    import time
    #imgages = []
    res = []
    for i in range(params['BATCH_SIZE']):
        #images.append(get_image.remote())
        img = get_image.remote()
        res.append(aggregator.remote(img, i))
    time.sleep(params['BATCH_INTERVAL'])
    return res

if __name__ == '__main__':
    from time import perf_counter

    initialize()

    # Load Images

    global img_models
    global num_models
    num_models = len(MODELS)
    img_models = []

    img_models.append(Resnet18.remote())
    img_models.append(Resnet50.remote())
    #img_models.append(Resnet101.remote())
    #img_models.append(BEiT.remote())
    #img_models.append(ConvNeXT.remote())
    #img_models.append(ViT384.remote())
    #img_models.append(MIT_B0.remote())

    
    '''
    img = get_image.remote()
    import time
    import os
    time.sleep(1)
    os.system('ray memory --stats-only')
    processed_img = preprocess.remote(img)
    time.sleep(1)
    os.system('ray memory --stats-only')
    res = []
    for model in img_models:
        res.append(model.predict.remote(img))

    for r in res:
        print(ray.get(r))

    res = []
    for model in img_models:
        res.append(model.predict.remote(processed_img))

    for r in res:
        print(ray.get(r))
   
    '''
    import os
    res = []
    start = perf_counter()
    for _ in range(params['NUM_BATCHES']):
        res.append(batch_submitter())
    os.system('ray memory --stats-only')

    models_run = 0
    for i in range(len(res)):
        for j in range(len(res[i])):
            models_run += ray.get(res[i][j])
    end = perf_counter()

    print(f"{params['NUM_BATCHES']} batches with {params['BATCH_SIZE']} requests : {end-start} ran {models_run} models")
