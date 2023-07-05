#!/usr/bin/env python
# coding: utf-8

# <!-- **Q1.** Given an array of integers nums and an integer target, return indices of the two numbers such that they add up to target.
# 
# You may assume that each input would have exactly one solution, and you may not use the same element twice.
# 
# You can return the answer in any order.
# 
# **Example:**
# Input: nums = [2,7,11,15], target = 9
# Output0 [0,1]
# 
# **Explanation:** Because nums[0] + nums[1] == 9, we return [0, 1][
#  --> -->

# In[1]:


def twoSum(nums, target):
    num_map = {}
    for i, num in enumerate(nums):
        complement = target - num
        if complement in num_map:
            return [num_map[complement], i]
        num_map[num] = i
    return []

# Example usage
nums = [2, 7, 11, 15]
target = 9
result = twoSum(nums, target)
print(result)


# In[ ]:


# 💡 **Q2.** Given an integer array nums and an integer val, remove all occurrences of val in nums in-place. The order of the elements may be changed. Then return the number of elements in nums which are not equal to val.

# Consider the number of elements in nums which are not equal to val be k, to get accepted, you need to do the following things:

# - Change the array nums such that the first k elements of nums contain the elements which are not equal to val. The remaining elements of nums are not important as well as the size of nums.
# - Return k.

# **Example :**
# Input: nums = [3,2,2,3], val = 3
# Output: 2, nums = [2,2,_*,_*]

# **Explanation:** Your function should return k = 2, with the first two elements of nums being 2. It does not matter what you leave beyond the returned k (hence they are underscores)


# In[3]:


nums = [3,2,2,3]
val = 3
while val in nums:
    nums.remove(val)
print(len(nums))


# In[ ]:


# 💡 **Q3.** Given a sorted array of distinct integers and a target value, return the index if the target is found. If not, return the index where it would be if it were inserted in order.

# You must write an algorithm with O(log n) runtime complexity.

# **Example 1:**
# Input: nums = [1,3,5,6], target = 5

# Output: 2


# In[4]:


# to Solve it with O(log n) complexity we need to apply binary search algo
def searchInsert(nums, target):
    left = 0
    right = len(nums) - 1
    
    while left <= right:
        mid = left + (right - left) // 2
        
        if nums[mid] == target:
            return mid
        elif nums[mid] < target:
            left = mid + 1
        else:
            right = mid - 1
    
    return left

# Example usage
nums = [1, 3, 5, 6]
target = 5
result = searchInsert(nums, target)
print(result)


# In[ ]:


# 💡 **Q4.** You are given a large integer represented as an integer array digits, where each digits[i] is the ith digit of the integer. The digits are ordered from most significant to least significant in left-to-right order. The large integer does not contain any leading 0's.

# Increment the large integer by one and return the resulting array of digits.

# **Example 1:**
# Input: digits = [1,2,3]
# Output: [1,2,4]

# **Explanation:** The array represents the integer 123.

# Incrementing by one gives 123 + 1 = 124.
# Thus, the result should be [1,2,4].


# In[5]:


def plusOne(digits):
    n = len(digits)
    
    # Start from the least significant digit
    for i in range(n - 1, -1, -1):
        # If the digit is less than 9, increment and return the result
        if digits[i] < 9:
            digits[i] += 1
            return digits
        
        # If the digit is 9, set it to 0 and continue to the next digit
        digits[i] = 0
    
    # If all digits are 9, add an extra digit 1 at the most significant position
    digits.insert(0, 1)
    
    return digits

# Example usage
digits = [1, 2, 3]
result = plusOne(digits)
print(result)


# In[ ]:


#  **Q5.** You are given two integer arrays nums1 and nums2, sorted in non-decreasing order, and two integers m and n, representing the number of elements in nums1 and nums2 respectively.

# Merge nums1 and nums2 into a single array sorted in non-decreasing order.

# The final sorted array should not be returned by the function, but instead be stored inside the array nums1. To accommodate this, nums1 has a length of m + n, where the first m elements denote the elements that should be merged, and the last n elements are set to 0 and should be ignored. nums2 has a length of n.

# **Example 1:**
# Input: nums1 = [1,2,3,0,0,0], m = 3, nums2 = [2,5,6], n = 3
# Output: [1,2,2,3,5,6]

# **Explanation:** The arrays we are merging are [1,2,3] and [2,5,6].
# The result of the merge is [1,2,2,3,5,6] with the underlined elements coming from nums1.


# In[6]:


def merge(nums1, m, nums2, n):
    i = m - 1  # Pointer for nums1
    j = n - 1  # Pointer for nums2
    k = m + n - 1  # Pointer for the merged array
    
    while i >= 0 and j >= 0:
        if nums1[i] > nums2[j]:
            nums1[k] = nums1[i]
            i -= 1
        else:
            nums1[k] = nums2[j]
            j -= 1
        k -= 1
    
    # Copy any remaining elements from nums2 to nums1
    while j >= 0:
        nums1[k] = nums2[j]
        j -= 1
        k -= 1

# Example usage
nums1 = [1, 2, 3, 0, 0, 0]
m = 3
nums2 = [2, 5, 6]
n = 3
merge(nums1, m, nums2, n)
print(nums1)


# In[ ]:


# Given an integer array nums, return true if any value appears at least twice in the array, and return false if every element is distinct.

# **Example 1:**
# Input: nums = [1,2,3,1]

# Output: true


# In[7]:


def containsDuplicate(nums):
    num_set = set()
    
    for num in nums:
        if num in num_set:
            return True
        num_set.add(num)
    
    return False

nums = [1, 2, 3, 1]
result = containsDuplicate(nums)
print(result)


# In[ ]:


# Given an integer array nums, move all 0's to the end of it while maintaining the relative order of the nonzero elements.

# Note that you must do this in-place without making a copy of the array.

# **Example 1:**
# Input: nums = [0,1,0,3,12]
# Output: [1,3,12,0,0]


# In[9]:


nums = [0,1,0,3,12]
n = nums.count(0)
val = 0
while val in nums:
    nums.remove(val)
while n!=0:
    nums.append(0)
    n=n-1
print(nums)


# In[ ]:


#  **Q8.** You have a set of integers s, which originally contains all the numbers from 1 to n. Unfortunately, due to some error, one of the numbers in s got duplicated to another number in the set, which results in repetition of one number and loss of another number.

# You are given an integer array nums representing the data status of this set after the error.

# Find the number that occurs twice and the number that is missing and return them in the form of an array.

# **Example 1:**
# Input: nums = [1,2,2,4]
# Output: [2,3]


# In[14]:


nums = [1,2,2,4]
n = len(nums)
lst = []
for i in range(n):
    if nums.count(nums[i]) > 1:
        a = nums[i]
print(a)
cur_sum = sum(nums)
corr_sum = n * (n + 1) // 2
diff = corr_sum - cur_sum
b = a + diff
lst.append(a)
lst.append(b)
print(lst)


# In[ ]:




