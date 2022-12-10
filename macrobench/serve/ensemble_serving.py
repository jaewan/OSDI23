import ray
from models import *
import random
import numpy as np

params=0
def get_params():  
    import argparse 
    global params
    parser = argparse.ArgumentParser()
    parser.add_argument('--NUM_BATCHES', '-nb', type=int, default=1)
    parser.add_argument('--BATCH_SIZE', '-bs', type=int, default=100)
    parser.add_argument('--BATCH_INTERVAL', '-bi', type=int, default=0)
    parser.add_argument('--RESULT_PATH', '-r', type=str, default="../data/dummy.csv")
    parser.add_argument('--OBJECT_STORE_SIZE', '-o', type=int, default=1_000_000_000)
    parser.add_argument('--MAX_MODEL_RUN', '-m', type=int, default=10)
    args = parser.parse_args()
    params = vars(args) 

@ray.remote
def get_image(i):
    from PIL import Image
    #idx = random.randint(0, 9)
    idx = i%12
    if idx == 0:
        image = Image.open(r'/dev/shm/Double-Cat-Wallpaper.jpg')
    elif idx == 1:
        image = Image.open(r'/dev/shm/qVnC9UJ.jpg')
    elif idx == 2:
        image = Image.open(r'/dev/shm/wp11786853.jpg')
    elif idx == 3:
        image = Image.open(r'/dev/shm/wp11177599.jpg')
    elif idx == 4:
        image = Image.open(r'/dev/shm/wp3878871.jpg')
    elif idx == 5:
        image = Image.open(r'/dev/shm/wp11335732.jpg')
    elif idx == 6:
        image = Image.open(r'/dev/shm/uwp3008684.jpeg')
    elif idx == 7:
        image = Image.open(r'/dev/shm/uwp2993171.jpeg')
    elif idx == 8:
        image = Image.open(r'/dev/shm/104801ab.jpg')
    elif idx == 9:
        image = Image.open(r'/dev/shm/219648fg.jpg')
    elif idx == 10:
        image = Image.open(r'/dev/shm/400853mt.jpg')
    elif idx == 11:
        image = Image.open(r'/dev/shm/911093ab.jpg')
    return image

@ray.remote(num_returns=2)
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
    return preprocessing(img), np.zeros(50_000_000 // 8)
                                                                                                         
def initialize():
    import os
    import json
    get_params()

    # Initialize Ray to spill to local disk
    spill_dir = os.getenv('RAY_SPILL_DIR')
    if spill_dir:
        ray.init(_system_config={"object_spilling_config": json.dumps({"type": "filesystem",
                                    "params": {"directory_path": spill_dir}},)}, object_store_memory=params['OBJECT_STORE_SIZE'])
        #ray.init(_system_config={"object_spilling_config": json.dumps({"type": "filesystem",
        #                            "params": {"directory_path": spill_dir}},)})
        print("Ray spill dir set")
    else:
        ray.init(object_store_memory=params['OBJECT_STORE_SIZE'])
        print("Ray default init")


def get_arbitrary_model():
    #return img_models[random.randint(0, num_models)]
    return img_models[random.randint(0, len(img_models) - 1)]


@ray.remote(num_cpus=1)
def aggregator(img, seq):
    random.seed(seq)
    INITIAL_MODEL_BATCH = 3
    processed_img, original_image = preprocess.remote(img)

    prediction_ref = []
    predictions = {}
    for _ in range(INITIAL_MODEL_BATCH):
        m = get_arbitrary_model()
        prediction_ref.append(m.simulate.remote(img, img, True))
    num_models_run = INITIAL_MODEL_BATCH

    for i in range(INITIAL_MODEL_BATCH):
        pred = ray.get(prediction_ref[i])
        predictions[pred] = predictions.get(pred,0) + 1
        if predictions[pred] >= ((num_models_run//2) + 1):
            return num_models_run

    vote = 0
    while vote < ((num_models_run//2) + 1) and num_models_run <= params['MAX_MODEL_RUN']:
        m = get_arbitrary_model()
        run_original_image = random.randint(0, 2)# params['MAX_MODEL_RUN']//2)
        pred = 0
        if run_original_image:
            pred = ray.get(m.simulate.remote(img, img, False))
        else:
            pred = ray.get(m.simulate.remote(processed_img, original_image, False))

        num_models_run += 1
        predictions[pred] = predictions.get(pred,0) + 1
        vote = predictions[pred]

    return num_models_run


def batch_submitter():
    import time
    res = []
    for i in range(params['BATCH_SIZE']):
        res.append(aggregator.remote(get_image.remote(i), i))
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
    img_models.append(Resnet101.remote())
    img_models.append(BEiT.remote())
    img_models.append(ConvNeXT.remote())
    img_models.append(ViT384.remote())
    img_models.append(MIT_B0.remote())

    
    '''
    for i in range(12):
        img = get_image.remote(i)
        import time
        import os
        time.sleep(1)
        os.system('ray memory --stats-only')
        processed_img, original_image = preprocess.remote(img)
        time.sleep(1)
        os.system('ray memory --stats-only')
        res = []
        for model in img_models:
            res.append(model.predict.remote(img))

        print('\t\t Original Image')
        for r in res:
            print(ray.get(r))

        res = []
        for model in img_models:
            res.append(model.predict.remote(processed_img))

        print('\t\t Preprocessed Image')
        for r in res:
            print(ray.get(r))
   
    '''
    import os
    res = []
    start = perf_counter()
    print('num batches', params['NUM_BATCHES'])
    print('batche size', params['BATCH_SIZE'])
    print('batche interval', params['BATCH_INTERVAL'])
    for _ in range(params['NUM_BATCHES']):
        res.append(batch_submitter())

    models_run = 0
    for i in range(len(res)):
        for j in range(len(res[i])):
            models_run += ray.get(res[i][j])
    end = perf_counter()

    os.system('ray memory --stats-only')
    print(f"{params['NUM_BATCHES']} batches with {params['BATCH_SIZE']} requests : {end-start} ran {models_run} models")
