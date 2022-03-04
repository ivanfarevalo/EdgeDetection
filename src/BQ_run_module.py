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
    input_img_path2 = input_path_dict['Input Image2']
    input_img_path3 = input_path_dict['Input Image3']
    input_csv_path = input_path_dict['Input CSV']
    input_csv_path2 = input_path_dict['Input CSV2']
    input_csv_path3 = input_path_dict['Input CSV3']
    input_npy_path = input_path_dict['Input Npy']
    input_npy_path2 = input_path_dict['Input Npy2']
    input_npy_path3 = input_path_dict['Input Npy3']

    # Read in data from each path
    img = cv2.imread(input_img_path, 0)
    img2 = cv2.imread(input_img_path2, 0)
    img3 = cv2.imread(input_img_path3, 0)
    csv = pd.read_csv(input_csv_path)
    csv2 = pd.read_csv(input_csv_path2)
    csv3 = pd.read_csv(input_csv_path3)
    npy = np.load(input_npy_path)
    npy2 = np.load(input_npy_path2)
    npy3 = np.load(input_npy_path3)

    
    ##### Run algorithm #####

    # Dummy case of returning files to output
    # edges_detected = canny_detector(img, min_hysteresis, max_hysteresis)
    
   
    ##### Save output #####
    
    # Get filename
    input_img_name = os.path.split(input_img_path)[-1][:-4]
    input_img_name2 = os.path.split(input_img_path2)[-1][:-4]
    input_img_name3 = os.path.split(input_img_path3)[-1][:-4]
    input_csv_name = os.path.split(input_csv_path)[-1][:-4]
    input_csv_name2 = os.path.split(input_csv_path2)[-1][:-4]
    input_csv_name3 = os.path.split(input_csv_path3)[-1][:-4]
    input_npy_name = os.path.split(input_npy_path)[-1][:-4]
    input_npy_name2 = os.path.split(input_npy_path2)[-1][:-4]
    input_npy_name3 = os.path.split(input_npy_path3)[-1][:-4]
    
    # Generate desired output file names and paths
    output_img_path = "%s/%s_out.jpg" % (output_folder_path, input_img_name)
    output_img_path2 = "%s/%s_out.jpg" % (output_folder_path, input_img_name2)
    output_img_path3 = "%s/%s_out.jpg" % (output_folder_path, input_img_name3)
    output_csv_path = "%s/%s_out.csv" % (output_folder_path, input_csv_name)
    output_csv_path2 = "%s/%s_out.csv" % (output_folder_path, input_csv_name2)
    output_csv_path3 = "%s/%s_out.csv" % (output_folder_path, input_csv_name3)
    output_npy_path = "%s/%s_out.npy" % (output_folder_path, input_npy_name)
    output_npy_path2 = "%s/%s_out.npy" % (output_folder_path, input_npy_name2)
    output_npy_path3 = "%s/%s_out.npy" % (output_folder_path, input_npy_name3)

    # Save output files
    cv2.imwrite(output_img_path, img)
    cv2.imwrite(output_img_path2, img2)
    cv2.imwrite(output_img_path3, img3)
    csv.to_csv(output_csv_path)
    csv2.to_csv(output_csv_path2)
    csv3.to_csv(output_csv_path3)
    np.save(output_npy_path, npy)
    np.save(output_npy_path2, npy2)
    np.save(output_npy_path3, npy3)

    # Create dictionary of output paths
    output_paths_dict = {}
    output_paths_dict['Image Out'] = output_img_path
    output_paths_dict['Image Out2'] = output_img_path2
    output_paths_dict['Image Out3'] = output_img_path3
    output_paths_dict['CSV Out'] = output_csv_path
    output_paths_dict['CSV Out2'] = output_csv_path2
    output_paths_dict['CSV Out3'] = output_csv_path3
    output_paths_dict['Npy Out'] = output_npy_path
    output_paths_dict['Npy Out2'] = output_npy_path2
    output_paths_dict['Npy Out3'] = output_npy_path3
    
    ##### Return output file path #####  -> Important step
    return output_paths_dict
    
if __name__ == '__main__':
    # Place some code to test implementation
     
    root_folder_path = os.path.dirname(os.path.abspath(__file__)) # Absolute path to parent directory 
    input_file_path = os.path.join(root_folder_path, 'bob.jpg')
    output_folder_path = root_folder_path

    output_file_path = run_module(input_file_path, output_folder_path, 100, 200)
    print("Output file saved to: %s" % output_file_path)
    
