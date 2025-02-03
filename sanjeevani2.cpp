
#include <iostream>
#include <fstream>
#include <sstream>
#include <map>
#include <string>
#include <cmath>
#include <ctime>
#include <iomanip>
#include <chrono>
#include <thread>
#include <vector>
#include <unordered_set>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <shared_mutex> // Required for std::shared_mutex

#include <queue>
#include <condition_variable>

using namespace std;

static int32_t buycounter;
static int32_t sellcounter;
static int32_t buyvalue;
static int32_t sellvalue;
struct StickData
{
    uint64_t time; // For comparison, assuming time is an integer
};
struct ParsedData
{
    uint64_t epoch_time;
    int token;
    double price;
    char order_traded_type;
    uint64_t exchange_ordernumber1;
    uint64_t exchange_ordernumber2;
    char buysellflag;
    double quantity;
};

// Define a structure to store token information along with volume
struct VolumeInfo
{
    double total_volume;
    double buyer_volume;
    double seller_volume;
    std::vector<int> tokens; // This will hold the tokens for the second
};

// Custom hash function for pairs
namespace std
{
    template <typename T1, typename T2>
    struct hash<std::pair<T1, T2>>
    {
        size_t operator()(const std::pair<T1, T2> &p) const
        {
            // Combine the hash values of the two elements of the pair
            auto h1 = std::hash<T1>{}(p.first);
            auto h2 = std::hash<T2>{}(p.second);

            // Combine the two hashes using bitwise XOR and shifting
            return h1 ^ (h2 << 1);
        }
    };
}

queue<ParsedData> dataQueue;

std::unordered_map<uint64_t, StickData> bsstick; // Stick data

std::unordered_map<int, std::unordered_map<uint64_t, VolumeInfo>> volumePerSecond;       // Token -> Second -> VolumeInfo
std::unordered_map<int, std::unordered_map<uint64_t, VolumeInfo>> buyerVolumePerSecond;  // Token -> Second -> BuyerVolumeInfo
std::unordered_map<int, std::unordered_map<uint64_t, VolumeInfo>> sellerVolumePerSecond; // Token -> Second -> SellerVolumeInfo
mutex queuemutex;
condition_variable cv;
const unsigned int max_buffer_size = 50;

std::map<std::pair<int, long long>, long long> buysidecurrentvaluemap;
std::map<std::pair<int, long long>, long long> sellsidecurrentvaluemap;

struct OHLCData
{
    double open = 0.0;
    double high = -INFINITY;
    double low = INFINITY;
    double close = 0.0;
    double total_volume = 0.0;
    double buyer_volume = 0.0;
    double seller_volume = 0.0;
};
std::unordered_map<int, std::map<long long, std::queue<std::pair<double, double>>>> openclose;
std::unordered_map<int, OHLCData> currentOHLC; // Token-specific OHLC data

template <typename K, typename V>

class ThreadSafeMap
{
    std::unordered_map<K, V> map_;
    mutable std::mutex mutex_;

public:
    V get(const K &key)
    {

        auto it = map_.find(key);
        return (it != map_.end()) ? it->second : V{};
    }

    void set(const K &key, const V &value)
    {

        map_[key] = value;
    }

    bool contains(const K &key)
    {

        return map_.find(key) != map_.end();
    }
};

bool parse_and_enqueue(const std::string &line)
{
    // cout << "In the parse enqueue line function\n";
    ParsedData parsedData;
    std::stringstream ss(line);
    std::string temp;

    try
    {
        // Parse fields from the line
        std::getline(ss, temp, ',');
        std::getline(ss, temp, ',');
        try
        {
            parsedData.epoch_time = std::stoull(temp);
        }
        catch (...)
        {
            return false; // Handle invalid parsing
        }
        std::getline(ss, temp, ',');
        std::getline(ss, temp, ',');
        std::getline(ss, temp, ',');
        std::getline(ss, temp, ',');
        parsedData.order_traded_type = temp[0];
        std::getline(ss, temp, ',');
        std::getline(ss, temp, ',');
        try
        {
            parsedData.exchange_ordernumber1 = std::stoull(temp);
        }
        catch (...)
        {
            return false; // Handle invalid parsing
        }

        std::getline(ss, temp, ',');
        if (parsedData.order_traded_type == 'T')
        {
            try
            {
                parsedData.exchange_ordernumber2 = std::stoull(temp);
            }
            catch (...)
            {
                return false; // Handle invalid parsing
            }
        }
        std::getline(ss, temp, ',');

        if (parsedData.order_traded_type == 'T')
        {
            try
            {
                parsedData.token = std::stoi(temp);
            }
            catch (...)
            {
                return false; // Handle invalid parsing
            }
        }
        if (parsedData.order_traded_type == 'M' || parsedData.order_traded_type == 'N')
        {
            parsedData.buysellflag = temp[0];
        }

        std::getline(ss, temp, ',');
        try
        {
            parsedData.price = std::stod(temp) / 100.0; // Convert to rupees
        }
        catch (...)
        {
            return false; // Handle invalid parsing
        }

        std::getline(ss, temp, ',');
        try
        {
            parsedData.quantity = std::stod(temp);
        }
        catch (...)
        {
            return false; // Handle invalid parsing
        }
        //  cout << "Queue size before pushing data in producer function is =" << dataQueue.size() << "\n";
        dataQueue.push(parsedData);
        //  cout << "Queue size After pushing data in producer function is =" << dataQueue.size() << "\n";
        return true;
    }
    catch (...)
    {
        return false; // Handle invalid parsing
    }
}
std::unordered_map<std::pair<uint64_t, uint64_t>, char, std::hash<std::pair<uint64_t, uint64_t>>> comparisonCache;

void find_stick(int token, const OHLCData &ohlc, uint64_t epoch_second, uint64_t exchange_ordernumber1, uint64_t exchange_ordernumber2, char &buyerSellerStick)
{
    static ThreadSafeMap<std::pair<uint64_t, uint64_t>, char> threadSafeCache;

    // Generate cache key
    const auto cache_key = std::make_pair(exchange_ordernumber1, exchange_ordernumber2);

    // Check cache first
    if (threadSafeCache.contains(cache_key))
    {
        buyerSellerStick = threadSafeCache.get(cache_key);
        return;
    }

    // Initialize stick type to 'N' (default)
    char stick_type = 'B';

    // Retrieve iterators for both exchange order numbers
    const auto it1 = bsstick.find(exchange_ordernumber1);
    const auto it2 = bsstick.find(exchange_ordernumber2);

    // Determine the stick type based on availability and time comparison
    if (it1 != bsstick.end() && it2 != bsstick.end())
    {
        // Both order numbers are found, compare times
        stick_type = (it1->second.time > it2->second.time) ? 'B' : 'S';
    }
    else if (it1 != bsstick.end())
    {
        // Only exchange_ordernumber1 is found
        stick_type = 'S';
    }
    else if (it2 != bsstick.end())
    {
        // Only exchange_ordernumber2 is found
        stick_type = 'B';
    }

    // Assign the result to the output parameter and cache it
    buyerSellerStick = stick_type;
    threadSafeCache.set(cache_key, stick_type);
}

std::string get_current_epoch_time_hhmmss() {
    // Get the current time in seconds since epoch
    auto now = std::chrono::system_clock::now();
    auto epoch_seconds = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();

    // Convert epoch seconds to time structure
    std::time_t time = static_cast<std::time_t>(epoch_seconds);
    std::tm* tm_info = std::localtime(&time); // Local time for current epoch time

    // Format the time into hh:mm:ss format
    std::ostringstream oss;
    oss << std::put_time(tm_info, "%H:%M:%S");  // hhmmss format

    return oss.str();
}

std::unordered_map<int, std::map<long long, std::queue<std::string>>> logQueue;
std::unordered_map<int, std::map<long long, std::pair<double, double>>> highLowValues;
void process_token_data(int token, double price, double quantity, uint64_t epoch_microseconds, uint64_t exchange_ordernumber1, uint64_t exchange_ordernumber2)
{
    uint64_t epoch_second = epoch_microseconds / 1000000; // Convert to seconds
    uint64_t current_epoch_second = std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();  
    auto &ohlc = currentOHLC[token];                      // Reference to the current OHLC data for this token

    // Update OHLC data
    if (ohlc.open == 0.0 && price != 0.0)
    {
        ohlc.open = ohlc.high = ohlc.low = price;
    }
    else
    {
        ohlc.high = std::max(ohlc.high, price);
        // Update low only if price is less than the current low
        if (price < ohlc.low || ohlc.low == 0.0)
        {
            ohlc.low = price;
        }
    }
    ohlc.close = price;
    ohlc.total_volume += quantity;
    // Determine buyer/seller stick type
    char buyerSellerStick = '\0';
    find_stick(token, ohlc, epoch_second, exchange_ordernumber1, exchange_ordernumber2, buyerSellerStick);

    if (buyerSellerStick == 'B')
    {
        ohlc.buyer_volume += quantity;
    }
    else if (buyerSellerStick == 'S')
    {
        ohlc.seller_volume += quantity;
    }

    // Log aligned time for the second
    uint64_t log_time = (epoch_microseconds / 1000000 * 1000000) + 1000; // Align to the nearest second
    std::time_t seconds = static_cast<std::time_t>(log_time / 1000000);
    std::tm *time_info = std::localtime(&seconds);
    std::ostringstream time_stream;
    time_stream << std::put_time(time_info, "%H:%M:%S");

    // Separate volume tracking for each token at the second level
    VolumeInfo &tot_info = volumePerSecond[token][epoch_second]; // Separate volume per token and second
    VolumeInfo &buyer_info = buyerVolumePerSecond[token][epoch_second];
    VolumeInfo &seller_info = sellerVolumePerSecond[token][epoch_second];

    // Update the total volume, buyer volume, seller volume
    tot_info.total_volume += ohlc.total_volume;
    buyer_info.buyer_volume += ohlc.buyer_volume;
    seller_info.seller_volume += ohlc.seller_volume;

    // Store the token in the respective volume info object
    tot_info.tokens.push_back(token);
    buyer_info.tokens.push_back(token);
    seller_info.tokens.push_back(token);

    std::pair<int, long long> key = std::make_pair(token, exchange_ordernumber1);
    if (exchange_ordernumber1 != 0)
    {

        buysidecurrentvaluemap[key] = buysidecurrentvaluemap[key] + (quantity * price);
        // cout << "Qty=" << quantity << std::setw(14) << "Price=" << price << std::setw(25) << "Exh_OrdNo1=" << exchange_ordernumber1 << std::setw(25) << "Buy_Side_Map____Value=" << buysidecurrentvaluemap[key] << "\n";
        if (buysidecurrentvaluemap[key] >= 10000000 && exchange_ordernumber1 != 0)
        {

            buycounter = buycounter + 1;
            buyvalue = buysidecurrentvaluemap[key];
            buysidecurrentvaluemap.erase(key);

            // cout << "Buy counter is Increased and Value is=" << buycounter << "\n";
        }

        // cout<<"Buy Side Value greater than 5 LAKH  completely traded Exchange_OrdNo1="<<exchange_ordernumber1<<"Value="<<buysidecurrentvaluemap[key]<<"\n";
    }

    std::pair<int, long long> key1 = std::make_pair(token, exchange_ordernumber2);
    if (exchange_ordernumber2 != 0)
    {

        sellsidecurrentvaluemap[key1] = sellsidecurrentvaluemap[key1] + (quantity * price);
        // cout << "Qty=" << quantity << std::setw(14) << "Price=" << price << std::setw(25) << "Exh_OrdNo2=" << exchange_ordernumber2 << std::setw(25) << "Sell_Side_Map____Value=" << sellsidecurrentvaluemap[key1] << "\n";
        if (sellsidecurrentvaluemap[key1] >= 10000000 && exchange_ordernumber2 != 0)
        {
            sellcounter = sellcounter + 1;
            sellvalue = sellsidecurrentvaluemap[key1];
            sellsidecurrentvaluemap.erase(key1);

            // cout << "Sell counter is Increased and Value is=" << sellcounter << "\n";
        }
        // cout<<"Sell Side Value greater than 5 LAKH  completely traded Exchange_OrdNo2="<<exchange_ordernumber2<<"Value="<<sellsidecurrentvaluemap[key1]<<"\n";
    }

    // Determine final stick type for this second
    char final_stick = '\0';
    if (ohlc.buyer_volume > ohlc.seller_volume)
    {
        final_stick = 'B';
    }
    else if (ohlc.buyer_volume < ohlc.seller_volume)
    {
        final_stick = 'S';
    }
    else
    {
        final_stick = 'D';
    }

    // Prepare output for this token and second
    std::ostringstream output;
    output << std::left
           << "Tok.: " << std::setw(5) << token
           << "| Time: " << std::setw(5) << time_stream.str()
           << "| TotVol.: " << std::setw(5) << tot_info.total_volume
           << "| BuyerVol.: " << std::setw(5) << buyer_info.buyer_volume
           << "| SellerVol.: " << std::setw(5) << seller_info.seller_volume
           << "| Stick: " << (final_stick == 'B' ? "B" : final_stick == 'S' ? "S"
                                                                            : "D")
           << "| BuyCtr: " << std::setw(5) << buycounter
           << "| SellCtr: " << std::setw(5) << sellcounter
           << "| BuyVal: " << std::setw(8) << buyvalue
           << "| SellVal: " << std::setw(8) << sellvalue;
    if (highLowValues.find(token) == highLowValues.end())
    {
        highLowValues[token][epoch_second] = {static_cast<double>(ohlc.high), static_cast<double>(ohlc.low)};
    }
    else
    {
        auto &currentHighLow = highLowValues[token][epoch_second];
        if (ohlc.high > currentHighLow.first)
        {
            currentHighLow.first = ohlc.high;
        }
        if (ohlc.low < currentHighLow.second || currentHighLow.second == 0.0)
        {
            currentHighLow.second = ohlc.low;
        }
    }
    logQueue[token][epoch_second].push(output.str());
    // Add entry to the queue for the current token and second
    std::pair<double, double> entry(ohlc.open, ohlc.close);
    openclose[token][epoch_second].push(entry);

    // Static variable to track the last printed second for each token
    static std::unordered_map<int, long long> last_print_second;
    if (current_epoch_second > last_print_second[token] && epoch_second != last_print_second[token])
    {

        // Print last entry of the queue for the last second for this token
        if (!logQueue[token][last_print_second[token]].empty())
        {
            auto first_entry = openclose[token][last_print_second[token]].front();
            auto last_entry = openclose[token][last_print_second[token]].back();
            std::ostringstream finalOutput;
            finalOutput << logQueue[token][last_print_second[token]].back()
                        << "| O: " << std::setw(8) << first_entry.first
                        << "| H: " << std::setw(8) << highLowValues[token][last_print_second[token]].first
                        << "| L: " << std::setw(8) << highLowValues[token][last_print_second[token]].second
                        << "| C: " << std::setw(8) << last_entry.second;
            std::cout << finalOutput.str() << std::endl;
            logQueue[token][last_print_second[token]].pop();
        }

        // Update last print second for this token
        last_print_second[token] = epoch_second;
        buycounter = 0;
        sellcounter = 0;
        sellvalue = 0;
        buyvalue = 0;
    }

    // Reset OHLC data for this token after processing
    ohlc = OHLCData();
}

void producer(const std::string &file_path)
{

    std::ifstream file(file_path);
    if (!file.is_open())
    {
        std::cerr << "Error opening file: " << file_path << std::endl;
        return;
    }
    // Move to the end of the file initially (to handle growing files)
    file.seekg(0, std::ios::end);
    while (1)
    {
        unique_lock<mutex> lock(queuemutex);

        cv.wait(lock, []
                { return dataQueue.size() < max_buffer_size; });
        std::string line;
        if (std::getline(file, line))
        {
            if (parse_and_enqueue(line))
            {
                // cout << "Producer-------------------------------*********************************************************-----------------------------------\n";
                //  Successfully parsed the line, process as needed
            }
            else
            {
                // std::cerr << ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Failed to parse line: " << line << std::endl;
            }
        }
        else
        {
            // No new data; wait before retrying
            std::this_thread::sleep_for(std::chrono::milliseconds(500)); // Adjust the sleep interval as needed
            file.clear();
        }
        lock.unlock();
        cv.notify_one();
    }
}

void update_ohlc(const ParsedData &data)
{
    uint64_t log_time = (data.epoch_time / 1000000 * 1000000) + 1000; // Align to the nearest second
    std::time_t seconds = static_cast<std::time_t>(log_time / 1000000);
    std::tm *time_info = std::localtime(&seconds);
    std::ostringstream time_stream;
    time_stream << std::put_time(time_info, "%H:%M:%S");
    // cout << "| Call process_token_data at: " << std::setw(5) << time_stream.str() << endl;
    process_token_data(data.token, data.price, data.quantity, data.epoch_time, data.exchange_ordernumber1, data.exchange_ordernumber2);
}

void update_bsstick(const ParsedData &data)
{

    bsstick[data.exchange_ordernumber1] = {data.epoch_time};
}

void consumer()
{

    while (true)
    {
        unique_lock<mutex> lock(queuemutex);

        cv.wait(lock, []
                { return dataQueue.size() > 0; });
        std::vector<ParsedData> batch;
        // cout << "Queue size=" << dataQueue.size() << "\n";
        while (!dataQueue.empty())
        {
            // Adjust batch size as needed
            batch.push_back(dataQueue.front());
            dataQueue.pop();
        }
        // cout << "Batch size is=" << batch.size() << "\n";
        for (const auto &data : batch)
        {

            // cout << "Consumer---------------------------" << "\n";
            if (data.order_traded_type == 'T')
            {
                update_ohlc(data);
            }
            else if (data.order_traded_type == 'N' || data.order_traded_type == 'M')
            {
                update_bsstick(data);
            }
        }

        lock.unlock();
        cv.notify_one();
    }
}
int main(int argc, char *argv[])
{
    if (argc < 2)
    {
        std::cerr << "Usage: " << argv[0] << " <input_file>" << std::endl;
        return 1;
    }

    std::string inputFilePath = argv[1];
    thread t1(producer, inputFilePath);
    thread t2(consumer);
    t1.join();
    t2.join();
}