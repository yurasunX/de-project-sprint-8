-- Run only in your own demonstration database, not the shared course source.
CREATE TABLE IF NOT EXISTS public.subscribers_restaurants (
    id serial4 PRIMARY KEY,
    client_id varchar NOT NULL,
    restaurant_id varchar NOT NULL,
    UNIQUE (client_id, restaurant_id)
);
INSERT INTO public.subscribers_restaurants (client_id, restaurant_id) VALUES
('223e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000'),
('323e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000'),
('423e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174001')
ON CONFLICT (client_id, restaurant_id) DO NOTHING;
