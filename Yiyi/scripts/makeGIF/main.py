import imageio
import os


def main():
    images = []
    imgdir = "/Users/tongyiyi/Documents/RBDA/RBDA_project/Yiyi/images_covid_aqi"
    for filename in sorted(os.listdir(imgdir)):
        if filename.endswith('.png'):
            file_path = os.path.join(imgdir, filename)
            images.append(imageio.imread(file_path))
    imageio.mimsave('/Users/tongyiyi/Documents/RBDA/RBDA_project/Yiyi/covid_aqi.gif', images, duration=1)


if __name__ == '__main__':
    main()
