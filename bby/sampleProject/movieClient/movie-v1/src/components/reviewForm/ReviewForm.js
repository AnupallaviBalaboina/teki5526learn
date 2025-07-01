import React from "react";
import { Form, Button } from "react-bootstrap";

const ReviewForm = ({ handleSubmit, revText, labelText, defaultValue = "" }) =>{
    return(
      <Form onSubmit={handleSubmit}>
        <Form.Group className="mb-3" controlId="exampleForm.ControlTextareal">
            <Form.Label>{labelText}</Form.Label>
            <Form.Control ref={revText} as="textarea" rows={3} defaultValue={defaultValue}/>
        </Form.Group>
        <Button variant="outline-info" >Submit</Button>
      </Form>
    )
}

export default ReviewForm