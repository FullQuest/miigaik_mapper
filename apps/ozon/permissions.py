"""Custom permissions."""

from rest_framework import permissions


class IsStaffOrAdmin(permissions.BasePermission):
    """Permissions admin or staff."""

    message = 'User has no writes to access view.'

    def has_permission(self, request, view):
        """Check user permissions."""
        try:
            if request.user.is_admin or request.user.is_staff:
                return True
        except AttributeError:
            return False

        return False
