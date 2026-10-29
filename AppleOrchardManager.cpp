#include <iostream>
#include <vector>
#include <random>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <chrono>

enum class AppleSize {
	Small,
	Medium,
	Large
};

enum class AppleColor {
	Green,
	Red,
	Golden,
	Pink,
	Purple
};

struct Apple {

	AppleSize size;
	AppleColor color;
};

int calculateApplePrice(const Apple& apple)
{
	// Determine base price from apple.size
	int basePrice = 0;

	switch (apple.size)
	{
	case AppleSize::Small:
		basePrice = 50;
		break;
	case AppleSize::Medium:
		basePrice = 80;
		break;
	case AppleSize::Large:
		basePrice = 120;
		break;
	default:
		std::cout << "Apple size invalid \n";
	}

	// Determine bonus from apple.color

	int colorBonus = 0;

	switch (apple.color)
	{
	case AppleColor::Green:
		colorBonus = 2;
		break;
	case AppleColor::Red:
		colorBonus = 10;
		break;
	case AppleColor::Golden:
		colorBonus = 50;
		break;
	case AppleColor::Pink:
		colorBonus = 80;
		break;
	case AppleColor::Purple:
		colorBonus = 90;
		break;
	default:
		std::cout << "Invalid apple color \n";

	}

	// Return the total price in cents

	return basePrice + colorBonus;
}

int calculateTruckRevenue(const std::vector<Apple>& appleTruck)
{
	int totalRevenue = 0;

	for (std::size_t i = 0; i < appleTruck.size(); i++)
	{
		totalRevenue += calculateApplePrice(appleTruck[i]);
	}

	return totalRevenue;
}

int main() {

	std::vector<Apple> pipeline;
	std::vector<Apple> appleTruck;
	std::mutex pipelineMutex;
	std::condition_variable cv;
	bool stopRequested = false;
	std::mutex stopMutex;


	const int truckSize = 200;

	bool producerFinished = false;

	int totalApplesProduced = 0;
	int totalApplesDispatched = 0;
	int totalTrucksDispatched = 0;

	auto producer = [&]()
	{
		std::random_device rd;
		std::mt19937 generator(rd());

		std::uniform_int_distribution<int> sizeDistribution(0, 2);
		std::uniform_int_distribution<int> colorDistribution(0, 4);
		std::uniform_int_distribution<int> batchDistribution(1, 250);
		std::uniform_int_distribution<int> sleepDistribution(500, 2000);


		while (true)
		{
			{
				std::lock_guard<std::mutex> guard(stopMutex);

				if (stopRequested)
				{
					break;
				}
			}
			int sleepDuration = sleepDistribution(generator);
			std::this_thread::sleep_for(std::chrono::milliseconds(sleepDuration));
			{
				std::lock_guard<std::mutex> guard(stopMutex);

				if (stopRequested)
				{
					break;
				}
			}
			int randomBatchSize = batchDistribution(generator);
			std::vector<Apple> batch;

			for (int i = 0; i < randomBatchSize; i++)
			{
				int randomSize = sizeDistribution(generator);
				int randomColor = colorDistribution(generator);

				AppleSize appleSize = static_cast<AppleSize>(randomSize);
				AppleColor appleColor = static_cast<AppleColor>(randomColor);

				Apple newApple{ appleSize, appleColor };
				batch.push_back(newApple);

			}
			{
				std::lock_guard<std::mutex> guard(pipelineMutex);
				for (const Apple& apple : batch)
				{
					pipeline.push_back(apple);
				}
			}
			totalApplesProduced += randomBatchSize;
			cv.notify_one();
		}

		{
			std::lock_guard<std::mutex> guard(pipelineMutex);
			producerFinished = true;
		}
		std::cout << "[PRODUCER] Production finished.\n";
		cv.notify_one();

	};

	auto consumer = [&]()
	{
		while (true)
		{
			std::unique_lock<std::mutex> lock(pipelineMutex);

			cv.wait(lock, [&] {
				return !pipeline.empty() || producerFinished;
				});
			/* Logic to dispatch the truck however full it is (after user quits) */
			if (producerFinished == true && pipeline.empty())
			{
				lock.unlock();
				if (!appleTruck.empty())
				{

					int currentTruckRevenue = calculateTruckRevenue(appleTruck);
					std::cout << "The revenue of this truck is: " << currentTruckRevenue << " cents. \n";
					std::cout << "Final partial truck dispatched! ("
						<< appleTruck.size() << "/" << truckSize
						<< " apples)\n";
					totalApplesDispatched += appleTruck.size();
					totalTrucksDispatched++;
					appleTruck.clear();
				}

				std::cout << "[CONSUMER] Pipeline drained. Exiting.\n";
				break;
			}

			while (!pipeline.empty() && appleTruck.size() < truckSize)
			{
				Apple apple = pipeline.back();
				appleTruck.push_back(apple);
				pipeline.pop_back();
			}

			lock.unlock();

			/* Logic to dispatch the truck when 100% full */
			if (appleTruck.size() == truckSize)
			{
				int currentTruckRevenue = calculateTruckRevenue(appleTruck);
				std::cout << "The revenue of this truck is: " << currentTruckRevenue << " cents. \n";
				std::cout << "Full truck dispatched! ("
					<< appleTruck.size() << "/" << truckSize
					<< " apples)\n";
				totalApplesDispatched += appleTruck.size();
				totalTrucksDispatched++;
				appleTruck.clear();
			}

		}

	};

	std::thread producerThread(producer);
	std::thread consumerThread(consumer);

	char input = {};
	while (std::cin >> input && input != 'q') {}

	{
		std::lock_guard<std::mutex> guard(stopMutex);
		stopRequested = true;

	}


	std::cout << "\n[MAIN] Shutdown requested.\n";


	producerThread.join();
	consumerThread.join();

	std::cout << "Total Apples Produced: " << totalApplesProduced << '\n';
	std::cout << "Total Apples Dispatched: " << totalApplesDispatched << '\n';
	std::cout << "Total Trucks Dispatched: " << totalTrucksDispatched << '\n';
	if (totalApplesProduced == totalApplesDispatched)
	{
		std::cout << "SUCCESS: All produced apples were dispatched!\n";
	}
	else
	{
		std::cout << "ERROR: Some apples were not dispatched!\n";
	}

	return 0;
}


