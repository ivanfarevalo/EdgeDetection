import pandas as pd
import cv2
import os
from detection import canny_detector
import numpy as np

# Tunable parameters must have defaults!
# input_path_dict will have input file paths with keys corresponding to the input names set in the cli.
def run_module(input_path_dict, output_folder_path, min_hysteresis=100, max_hysteresis=200): 

    ##### Preprocessing #####

    # Get input file paths from dictionary
    input_img_path = input_path_dict['Input Image']
    input_csv_path = input_path_dict['Input CSV']
    input_npy_path = input_path_dict['Input Npy']

    # Read in data from each path
    img = cv2.imread(input_img_path, 0)
    csv = pd.read_csv(input_csv_path)
    npy = np.load(input_npy_path)

    
    ##### Run algorithm #####

    # Dummy case of returning files to output
    # edges_detected = canny_detector(img, min_hysteresis, max_hysteresis)
    
   
    ##### Save output #####
    
    # Get filename
    input_img_name = os.path.split(input_img_path)[-1][:-4]
    input_csv_name = os.path.split(input_csv_path)[-1][:-4]
    input_npy_name = os.path.split(input_npy_path)[-1][:-4]
    
    # Generate desired output file names and paths
    output_img_path = "%s/%s_out.jpg" % (output_folder_path, input_img_name)
    output_csv_path = "%s/%s_out.csv" % (output_folder_path, input_csv_name)
    output_npy_path = "%s/%s_out.npy" % (output_folder_path, input_npy_name)

    # Save output files
    cv2.imwrite(output_img_path, img)
    csv.to_csv(output_csv_path)
    np.save(output_npy_path, npy)

    # Create dictionary of output paths
    output_paths_dict = {}
    output_paths_dict['Image Out'] = output_img_path
    output_paths_dict['CSV Out'] = output_csv_path
    output_paths_dict['Npy Out'] = output_npy_path
    
    ##### Return output file path #####  -> Important step
    return output_paths_dict
    
if __name__ == '__main__':
    # Place some code to test implementation
     
    root_folder_path = os.path.dirname(os.path.abspath(__file__)) # Absolute path to parent directory 
    input_file_path = os.path.join(root_folder_path, 'bob.jpg')
    output_folder_path = root_folder_path

    output_file_path = run_module(input_file_path, output_folder_path, 100, 200)
    print("Output file saved to: %s" % output_file_path)
    
