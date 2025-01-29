import logging
from typing import Dict, Any
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from concurrent.futures import TimeoutError as FutureTimeoutError

class NoSuchUserError(Exception):
    """Custom exception for when a user is not found."""
    def __init__(self, username: str):
        super().__init__(f"User '{username}' not found.")
        self.username = username

class DatabaseOperationError(Exception):
    """Custom exception for database failures."""
    pass

class BusinessNumbers:
    def __init__(self, dbwrapper):
        """
        Initialize the business logic with enhanced robustness.

        Args:
            dbwrapper: Database wrapper with transactional support.
        """
        self.dbwrapper = dbwrapper
        self.log = logging.getLogger(__name__)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(1),
        retry=retry_if_exception_type((FutureTimeoutError, DatabaseOperationError)))
    
    def donate_kbase_points(self, username: str, magic_kbase_points: int) -> Dict[str, Any]:
        """
        Donate KBase points with atomicity, retries, and timeout handling.

        Args:
            username: User donating points.
            magic_kbase_points: Positive integer points to donate.

        Returns:
            Dict with username, remaining points, and update ID.

        Raises:
            TypeError: If input type is invalid.
            ValueError: If points are <=0 or insufficient.
            NoSuchUserError: If user not found.
            DatabaseOperationError: For persistent database failures.
        """
        
        # Validate input type and value
        if not isinstance(magic_kbase_points, int):
            self.log.error(f"TypeError: Expected int, got {type(magic_kbase_points)}")
            raise TypeError("Donation points must be an integer.")
        if magic_kbase_points <= 0:
            self.log.error(f"ValueError: Invalid points {magic_kbase_points}")
            raise ValueError("Donation points must be a positive integer.")

        try:
            # Start atomic transaction
            with self.dbwrapper.transaction():
                # Fetch user with timeout
                user = self.dbwrapper.get_user(username, timeout=2)
                if user is None:
                    self.log.warning(f"NoSuchUserError: {username}")
                    raise NoSuchUserError(username)

                # Check balance
                if user.kbase_points < magic_kbase_points:
                    self.log.error(
                        f"Insufficient points: User {username} has {user.kbase_points}, "
                        f"needs {magic_kbase_points}"
                    )
                    raise ValueError("Insufficient KBase points.")

                # Atomic update
                user.kbase_points -= magic_kbase_points
                update_id = self.dbwrapper.save_user(user)

                self.log.info(
                    "Donation success",
                    extra={"username": username, 
                           "donated_points": magic_kbase_points, 
                           "remaining_points": user.kbase_points, 
                           "update_id": update_id}
                    )
                
                return {
                    "username": username,
                    "remaining_kbase_points": user.kbase_points,
                    "update_id": update_id
                }

        except NoSuchUserError:
            raise  # Re-raise known exceptions
        except ValueError as e:
            self.log.error(f"ValueError: {e}")
            raise
        except Exception as e:
            self.log.critical(f"DatabaseOperationError: {str(e)}", exc_info=True)
            raise DatabaseOperationError("Failed to process donation after retries.") from e 