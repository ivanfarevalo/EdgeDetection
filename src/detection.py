import cv2 as cv

def canny_detector(img, min_h, max_h):

	return cv.Canny(img, min_h, max_h)

if __name__ == '__main__':
	
	img_path = 'bob.jpg'
	img = cv.imread(img_path)
	
	edges = canny_detector(img, 100, 200)
	cv.imwrite('out.jpg', edges) 
