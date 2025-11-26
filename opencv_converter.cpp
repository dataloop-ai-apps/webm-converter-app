#include <iostream> // for standard I/O
#include <string>   // for strings
#include <opencv2/imgcodecs.hpp>
#include <opencv2/core.hpp>    // Basic OpenCV structures (cv::Mat)
#include <opencv2/videoio.hpp> // Video write
#include <chrono>

using namespace std;
using namespace cv;

int main(int argc, char *argv[])
{
    const string source = argv[1];
    const string NAME = argv[2];

    VideoCapture inputVideo(source); // Open input
    if (!inputVideo.isOpened())
    {
        cout << "Could not open the input video: " << source << endl;
        return -1;
    }
    int ex = static_cast<int>(808996950);             // Get Codec Type: VP8

    Size S = Size((int)inputVideo.get(CAP_PROP_FRAME_WIDTH),
                  (int)inputVideo.get(CAP_PROP_FRAME_HEIGHT));

    VideoWriter outputVideo; // Open the output
    outputVideo.open(NAME, ex, inputVideo.get(CAP_PROP_FPS), S, true);

    if (!outputVideo.isOpened())
    {
        cout << "Could not open the output video for write: " << source << endl;
        return -1;
    }

    Mat src;

    for (;;)
    {
        inputVideo >> src; // read
        if (src.empty())
        {
            break;
        }
        outputVideo << src;
    }
    return 0;
}
