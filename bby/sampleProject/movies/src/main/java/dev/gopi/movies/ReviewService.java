package dev.gopi.movies;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

@Service
public class ReviewService {

    @Autowired
    private ReviewRepository reviewRepository;

    @Autowired
    private MongoTemplate mongoTemplate;
  
    public Review createReview(String reviewBody, String imdbId){ 
        Review review = new Review(reviewBody);
        reviewRepository.insert(review);
      Query query = new Query(Criteria.where("imdbId").is(imdbId));
      Update update = new Update().push("reviewIds", review);
      mongoTemplate.updateFirst(query, update, Movie.class);

    
        return review;

    }


}
