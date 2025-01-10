SELECT * FROM usersubmissions;

SELECT username, COUNT(username) AS total_submissions, SUM(points) as total_points
FROM usersubmissions
GROUP BY username;


SELECT username, DATE(submitted_at) AS Day, AVG(points) AS avg_points
FROM usersubmissions
GROUP BY username, Day
ORDER BY username, Day;

SELECT * FROM usersubmissions;

SELECT username, Day, total_points, Rankings
FROM (
    SELECT username, 
           DATE(submitted_at) AS Day, 
           SUM(points) AS total_points,
           RANK() OVER (PARTITION BY DATE(submitted_at) ORDER BY SUM(points) DESC) AS Rankings
    FROM usersubmissions
    GROUP BY username, Day
) AS top3
WHERE Rankings <= 3 AND total_points > 0
ORDER BY Day, Rankings;


SELECT username, Day, total_points, Rankings
FROM (
    SELECT username, 
           DATE(submitted_at) AS Day, 
           SUM(points) AS total_points,
           RANK() OVER (PARTITION BY DATE(submitted_at) ORDER BY SUM(points) ASC) AS Rankings
    FROM usersubmissions
    GROUP BY username, Day
) AS bottom5
WHERE Rankings <= 5
ORDER BY Day, Rankings;

SELECT username, WEEK, total_points, Rankings
FROM (
    SELECT username, 
           WEEK(submitted_at, 1) AS WEEK, 
           SUM(points) AS total_points,
           RANK() OVER (PARTITION BY WEEK(submitted_at, 1) ORDER BY SUM(points) DESC) AS Rankings
    FROM usersubmissions
    GROUP BY username, WEEK
) AS top10
WHERE Rankings <= 10 AND total_points >= 0
ORDER BY WEEK, Rankings;

















