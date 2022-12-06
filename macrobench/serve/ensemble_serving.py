import ray                                                                                               
from models import *                                                                                     
from datasets import load_dataset                                                                        
import random                                                                                            
                                                                                                         
params=0                                                                                                 
dataset = 0                                                                                              
def get_params():                                                                                        
    import argparse                                                                                      
    global params                                                                                                                                                                                                  
    parser = argparse.ArgumentParser()                                                                   
    parser.add_argument('--NUM_BATCHES', '-nb', type=int, default=1)                                     
    parser.add_argument('--BATCH_SIZE', '-bs', type=int, default=1)                                      
    parser.add_argument('--BATCH_INTERVAL', '-bi', type=int, default=1)                                  
    parser.add_argument('--RESULT_PATH', '-r', type=str, default="../data/dummy.csv")                    
    args = parser.parse_args()                                                                           
    params = vars(args)                                                                                  
                                                                                                         
                                                                                                         
@ray.remote                                                                                              
def get_image():                                                                                         
    '''                                                                                                  
        image = dataset["test"]["image"][0]                                                              
                                                                                                         
        url = 'http://images.cocodataset.org/val2017/000000039769.jpg'                                   
        image = Image.open(requests.get(url, stream=True).raw)
    '''                                                                                                  
    image = dataset["test"]["image"][0]                                                                  
    return image                                                                                         
                                                                                                         
def initialize():
    import os
    import json
    get_params()

    # Initialize Ray to spill to local disk
    spill_dir = os.getenv('RAY_SPILL_DIR')
    if spill_dir:
        ray.init(_system_config={"object_spilling_config": json.dumps({"type": "filesystem",
                                    "params": {"directory_path": spill_dir}},)})
        print("Ray spill dir set")
    else:
        ray.init()
        print("Ray default init")

    # Load image libraries
    global dataset
    dataset = load_dataset("huggingface/cats-image")

def get_arbitrary_model():
    #return img_models[random.randint(0, num_models)]
    return random.randint(0, 1)


@ray.remote
def aggregator(img, seq):
    random.seed(seq)
    INITIAL_MODEL_BATCH = 3

    prediction_ref = []
    predictions = {}
    for _ in range(INITIAL_MODEL_BATCH):
        m = get_arbitrary_model()
        prediction_ref.append(m.predict.remote(img))
    num_models_run = INITIAL_MODEL_BATCH

    for i in range(INITIAL_MODEL_BATCH):
        pred = ray.get(prediction_ref[i])
        predictions[pred] = predictions.get(pred,0) + 1
        if predictions[pred] >= ((num_models_run//2) + 1):
            return num_models_run

    vote = 0
    while vote < ((num_models_run//2) + 1):
        m = get_arbitrary_model()
        pred = ray.get(m.predict.remote(img))
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
        res.append(aggregator.remote(get_image.remote(), i))
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
    img_models.append(BEiT.remote())
    img_models.append(Resnet50.remote())
    img_models.append(Resnet101.remote())
    img_models.append(ConvNeXT.remote())
    img_models.append(ViT384.remote())

    
    res = []
    start = perf_counter()
    for _ in range(params['NUM_BATCHES']):
        res.append(batch_submitter())

    models_run = 0
    for i in range(len(res)):
        for j in range(len(res[i])):
            models_run += ray.get(res[i][j])
    end = perf_counter()

    print(f"{params['NUM_BATCHES']} batches with {params['BATCH_SIZE']} requests : {end-start} ran {models_run} models")
