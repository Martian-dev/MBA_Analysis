# Coupon delivery and Flask integration

Type: prototype
Status: open
Blocked by: 07

## Question

How should the existing Flask app consume or expose coupon-issued events and operational stream evidence without letting the browser invent discount eligibility? The provisional Signal Room uses a bounded Redis activity stream and Server-Sent Events; this ticket must validate that contract for the distributed implementation.
