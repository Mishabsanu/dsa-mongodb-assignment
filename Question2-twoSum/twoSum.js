function twoSum(nums, target) {
  const map = new Map();

  for (let i = 0; i < nums.length; i++) {
    const complement = target - nums[i];

    if (map.has(complement)) {
      return [map.get(complement), i];
    }

    map.set(nums[i], i);
  }

  throw new Error("No valid two sum solution found");
}

const nums = [2, 7, 11, 15];
const target = 9;
try {
  console.log(twoSum(nums, target));
} catch (error) {
  console.error(error.message);
}
