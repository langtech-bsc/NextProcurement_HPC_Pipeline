import poppler
from pdf2image import convert_from_path
import cv2
from PIL import Image
import pytesseract
import matplotlib.pyplot as plt
import os
import numpy as np
import shutil


## MACROS FOR CONTROL OVERFLOWS

MAX_PLT_OPENED_FIGS = 200 # plt
# Avoiding DecompressionBombWarning
Image.MAX_IMAGE_PIXELS = None



def overlap(c1, c2):
    """
     ____
    |    |    _____   : not overlap
    |____|   |     |  : overlap
             |_____|  : not overlap

    The overlap of 2 boxes is defined as the percentage of anyvertical edge shared between the boxes.
    """
    c1_y0 = c1[0][1]
    c1_y1 = c1[1][1]
    c2_y0 = c2[0][1]
    c2_y1 = c2[1][1]
    shared = max(0, min(c1_y1, c2_y1) - max(c1_y0, c2_y0))
    min_edge = min(c1_y1 - c1_y0, c2_y1 - c2_y0) 
    overlap = 0 if min_edge == 0 else shared/min_edge
    return overlap

def merge(c1, c2):
    """
     ____                   _____________
    |    |    _____        |             |
    |____|   |     |  ==>  |             |
             |_____|       |_____________|
    """
    x0 = min(c1[0][0], c2[0][0])
    x1 = max(c1[1][0], c2[1][0])
    y0 = min(c1[0][1], c2[0][1])
    y1 = max(c1[1][1], c2[1][1])
    merged_c = [(x0, y0), (x1, y1)]
    return merged_c

def mark_region(opencvImage):
    
    #im = cv2.imread(image_path)
    im = opencvImage

    gray = cv2.cvtColor(im, cv2.COLOR_BGR2GRAY)
    blur = cv2.GaussianBlur(gray, (9,9), 0)
    thresh = cv2.adaptiveThreshold(blur,255,cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY_INV,11,30)

    # Dilate to combine adjacent text contours
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (9,9))
    dilate = cv2.dilate(thresh, kernel, iterations=4)

    # Find contours, highlight text areas, and extract ROIs
    cnts = cv2.findContours(dilate, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    cnts = cnts[0] if len(cnts) == 2 else cnts[1]

    line_items_coordinates = []
    for c in cnts:
        area = cv2.contourArea(c)
        x,y,w,h = cv2.boundingRect(c)

        if area > 10000:
            line_items_coordinates.append([(x,y), (x+w, y+h)])

    # join overlapping edges
    line_items_coordinates = sorted(line_items_coordinates, key=lambda l: l[0][1])
    new_items_coordinates = []
    for i in range(len(line_items_coordinates)):
        c2 = line_items_coordinates[i]
        if len(new_items_coordinates) == 0:
            new_items_coordinates.append(c2)
        else:
            c1 = new_items_coordinates[-1]
            if overlap(c1, c2) > 0.5:
                new_items_coordinates[-1] = merge(c1, c2)
            else:
                new_items_coordinates.append(c2)
    # draw rectangles
    #  for c in new_items_coordinates:
    #      image = cv2.rectangle(im, c[0], c[1], color=(255,0,255), thickness=3)

    return new_items_coordinates

def plot_image(image):
    plt.rcParams.update({'figure.max_open_warning': MAX_PLT_OPENED_FIGS})

    plt.figure(figsize=(10,10))
    plt.imshow(image)
    plt.show()

def get_text_from_image(image):
    # pytesseract image to string to get results
    text = str(pytesseract.image_to_string(image, config='--psm 6', lang="spa+cat+eus+glg"))
    return text


def _convert_pil_to_cv2_image(pil_image):
    '''This should convert it'''
    return cv2.cvtColor(np.array(pil_image), cv2.COLOR_RGB2BGR)


def get_paragraphs_from_pdf(pdf_path):
    """ Given a pdf path returns all paragraphs in that pdf

    """

    #pages = convert_from_path(pdf_path, 350, poppler_path = os.path.dirname(poppler.__file__))
    list_of_pil_images = convert_from_path(pdf_path, 350, poppler_path = os.path.dirname(shutil.which("pdftoppm")))
    list_of_openvc_images = [_convert_pil_to_cv2_image(i) for i in list_of_pil_images]


    page_names = []
    i = 1
    all_paragraphs = []
    #for img_path in page_names:
    #for pil_image in list_of_pil_images:
    for opencvImage in list_of_openvc_images:

        paragraphs = []
        is_table = -1

        #opencvImage = _convert_pil_to_cv2_image(pil_image)
        line_items_coordinates = mark_region(opencvImage)

        for c in sorted(line_items_coordinates, key=lambda l: l[0][1]):
            # cropping image img = image[y0:y1, x0:x1]
            img = opencvImage[c[0][1]:c[1][1], c[0][0]:c[1][0]]
            # convert the image to black and white for better OCR
            _,thresh1 = cv2.threshold(img,120,255,cv2.THRESH_BINARY)
            text = get_text_from_image(thresh1)
            is_table = detect_table(img)
            paragraphs.append([text, c, is_table])


        all_paragraphs.append(paragraphs)


    return all_paragraphs





def detect_table(img):
    """ 
        Given an image return true if it is a table
    """



    if(np.array(img).shape == (0,0)):
        return False

#     blur = cv2.GaussianBlur(gray, (9,9), 0)
#     thresh = cv2.adaptiveThreshold(blur,255,cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY_INV,11,30)
#     kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (2, 2))
#     vertical_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, np.array(img).shape[1]//150))
#     plotting = plt.imshow(img,cmap='gray')
#     plt.title("Inverted Image with otsu thresh holding")
#     plt.show()

#     plotting = plt.imshow(img)
#     plt.title(" Image ")
#     plt.show()
#     import pdb; pdb.set_trace()
    img=cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    img_bin1 = 255-img
    thresh1,img_bin1_otsu = cv2.threshold(img_bin1,128,255,cv2.THRESH_OTSU)
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (2, 2))
    vertical_kernel_height = max(1, img.shape[0] // 100)

    vertical_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, vertical_kernel_height))
    eroded_image = cv2.erode(img_bin1_otsu, vertical_kernel, iterations=3)
    vertical_lines = cv2.dilate(eroded_image, vertical_kernel, iterations=3)      

    hor_kernel_width = max(1, np.array(img).shape[1]//100  )
    hor_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, ( hor_kernel_width , 1))
    horizontal_lines = cv2.erode(img_bin1_otsu, hor_kernel, iterations=5)
    
#     # Count vertical lines:
#     vertical_contours, _ = cv2.findContours(vertical_lines, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)
#     # Count horizontal lines
#     horizontal_contours, _ = cv2.findContours(horizontal_lines, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)
#     modified_h_coords = []
#     for h_contour in horizontal_contours:
#         x, y, w, h = cv2.boundingRect(h_contour)
# #         x = x - w
# #         w = 3*w
# #         cv2.rectangle(img,(x,y),(x+w,y+h),(0,255,0),2)
#         modified_h_coords.append((x, y, x+w, y+h))
#     modified_v_coords = []
#     for v_contour in vertical_contours:
#         x, y, w, h = cv2.boundingRect(v_contour)
# #         y = y - h
# #         h = 3 * h
# #         cv2.rectangle(img,(x,y),(x+w,y+h),(0,255,0),2)
#         modified_v_coords.append((x, y, x+w, y+h))
#     # detect intersections
#     num_intersections = 0
#     for (h_x, h_y, h_x2, h_y2) in modified_h_coords:
#         for (v_x, v_y, v_x2, v_y2) in modified_v_coords:
#             if ((v_x >= h_x and v_x <= h_x2) or (v_x2 >= h_x and v_x2 <= h_x2)) and \
#                 ((h_y <= v_y2 and h_y >= v_y) or (h_y2 <= v_y2 and h_y2 >= v_y)):
#                     # there is intersection
#                     num_intersections += 1
# 
#     return num_intersections
# 










#     if len(vertical_contours) > 2 and len(horizontal_contours) > 1:
    vertical_horizontal_lines = cv2.addWeighted(vertical_lines, 0.5, horizontal_lines, 0.5, 0.0)
    vertical_horizontal_lines = cv2.erode(~vertical_horizontal_lines, kernel, iterations=3)
    thresh, vertical_horizontal_lines = cv2.threshold(vertical_horizontal_lines,128,255, cv2.THRESH_BINARY | cv2.THRESH_OTSU)

#     se = np.ones((50,1), dtype='uint8')
#     image_close = cv2.morphologyEx(vertical_horizontal_lines, cv2.MORPH_CLOSE, se)
#     import pdb; pdb.set_trace()



#     plotting = plt.imshow(img)
# #     plt.title("")
#     plt.show()
#     return 0


    

#     bitxor = cv2.bitwise_xor(img,vertical_horizontal_lines)
#     bitnot = cv2.bitwise_not(bitxor)

# 
#     contours, hierarchy = cv2.findContours(vertical_horizontal_lines, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)
#     boundingBoxes = [cv2.boundingRect(contour) for contour in contours]
#     (contours, boundingBoxes) = zip(*sorted(zip(contours, boundingBoxes),key=lambda x:x[1][1]))
#     boxes = []
# #     for contour in contours:
# #         x, y, w, h = cv2.boundingRect(contour)
# #         area = cv2.contourArea(contour)
# #         if area > 1000:
# #             cv2.rectangle(img,(x,y),(x+w,y+h),(0,255,0),2)
# #             boxes.append([x,y,w,h])
# 
#     plotting = plt.imshow(img)
#     plt.title("Identified contours")
#     plt.show()

#         import pdb; pdb.set_trace()
#     return len(boxes)
       
#     else:
#         return 0
# 


#     plt.figure(figsize= (30,30))
#     plt.subplot(151),plt.imshow(eroded_image, cmap = 'gray')
#     plt.title('Image after erosion with vertical kernel'), plt.xticks([]), plt.yticks([])

#     plotting = plt.imshow(img_bin1_otsu,cmap='gray')
#     plt.title("Inverted Image with otsu thresh holding")
#     plt.show()


#     gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    ret,thresh_value = cv2.threshold(vertical_horizontal_lines,180,255,cv2.THRESH_BINARY_INV)
    kernel = np.ones((5,5),np.uint8)
    dilated_value = cv2.dilate(thresh_value,kernel,iterations = 1)
    contours, hierarchy = cv2.findContours(dilated_value,cv2.RETR_TREE,cv2.CHAIN_APPROX_SIMPLE)
#     cordinates = []
    img_size = vertical_horizontal_lines.shape[0] * vertical_horizontal_lines.shape[1]
    is_table = False
    for cnt in contours:
        x,y,w,h = cv2.boundingRect(cnt)
        contour_size = w*h
        if contour_size > 0.75*img_size:
            is_table = True
#         cordinates.append((x,y,w,h))
#         cv2.rectangle(img,(x,y),(x+w,y+h),(0,0,255),1)
#     plt.imshow(img)
#     plt.show()
#     import pdb; pdb.set_trace()
    return is_table




def get_results_visualization(image_name, image, paragraphs_coords_and_label):
    # 
    font                   = cv2.FONT_HERSHEY_SIMPLEX
    fontScale              = 1.5
    fontColor              = (100,100,100)
    thickness              = 3
    lineType               = 2
    for c, label in paragraphs_coords_and_label:
        # Initialize blank mask
        if label == "Table":
            cv2.rectangle(image, c[0], c[1], (0,255,0), thickness=5)
            cv2.putText(image,'TABLE', 
                (c[0][0] - 230, c[0][1] + 50), 
                font, 
                fontScale,
                fontColor,
                thickness,
                lineType)
        elif "Heading" == label[0]["label"]:
            # Draw rectangles
            cv2.rectangle(image, c[0], c[1], (255,153,0), thickness=5)
            cv2.putText(image,'HEADING', 
                (c[0][0] - 230, c[0][1] + 50), 
                font, 
                fontScale,
                fontColor,
                thickness,
                lineType)
        else:
            cv2.rectangle(image, c[0], c[1], (0,153,255), thickness=5)
            cv2.putText(image,'DEFAULT', 
                (c[0][0] - 230, c[0][1] + 50), 
                font, 
                fontScale,
                fontColor,
                thickness,
                lineType)
    plot_image(image)
    # save
    # TODO
    cv2.imwrite(image_name, image)

"""
if __name__ == "__main__":
    pdf_path = r"../../pdf_to_struct/ntp00000014_Pliego_Prescripciones_tecnicas_URI.pdf"
    # pdfs = r"ntp00000055_Pliego_clausulas_administrativas_URI.pdf"

    paragraphs = get_paragraphs_from_pdf(pdf_path)
    for text in paragraphs:
        print(text)
    
"""


        
